<?php

namespace Wyz\PathProcessor;

use Psr\Log\LoggerInterface;
use RuntimeException;

/**
 * Does processing (which is not defined in this class) on files.
 *
 * The general idea is to construct a new class (with the needed config) and
 * then call processPaths() on all files/directories that need to be processed
 * (recursively). validatePath() may be called but beforehand but this should
 * generally not be necessary, because validation is done in processPaths().
 */
class PathProcessor
{
    /**
     * Logger.
     *
     * @var \Psr\Log\LoggerInterface
     */
    private $logger;

    /**
     * Configuration values.
     *
     * If we want behavior to be configurable, we'll just refer to a key inside
     * this array. We won't do much about populating it though, so
     * - there is no getter / option for a default value (yet?), so it's up to
     *   the code to check if things are set or up to the constructor to always
     *   populate a value.
     * - there are just defaults for certain behavior spead around the code.
     *
     * To see what configuration options are available, check the code. (Often
     * they are mentioned in the constructor.)
     *
     * @var array
     */
    protected $config;

    /**
     * State values.
     *
     * For classes to put any value here that they need to remember. There's a
     * getter for access by outside code, or as a 'shorthand' for when you're
     * not sure if the state value exists, or you need a default... but it's
     * perfectly allowed to read/write directly to this variable from a class.
     *
     * @var array
     */
    protected $state;

    /**
     * PathProcessor constructor.
     *
     * @param \Psr\Log\LoggerInterface $logger
     *   A logger instance.
     * @param array $config
     *   (Optional) configuration values.
     */
    public function __construct(LoggerInterface $logger, array $config = [])
    {
        if (isset($config['base_directory'])) {
            if (!is_string($config['base_directory'])) {
                throw new RuntimeException("The 'base_directory' configuration value must be a string.");
            }
            if (!is_dir($config['base_directory'])) {
                throw new RuntimeException("The 'base_directory' configuration value is not an existing directory.");
            }
        }
        $this->config = $config + [
                // By default, symlinks are not processed, because we dont want
                // to make assumptions about what we can do with them; they
                // will cause errors to be logged. Set this to true to pass all
                // symlinks into processFile() instead. Child classes may also
                // support setting this to a non-empty string, to distinguish
                // various types of processing of symlinks.
                'process_symlinks' => false,
            ];

        $this->state = ['errors' => 0, 'symlinks_skipped' => 0];
        $this->logger = $logger;
    }

    /**
     * Returns the logger.
     *
     * @return \Psr\Log\LoggerInterface
     */
    public function getLogger()
    {
        return $this->logger;
    }

    /**
     * Returns a state value.
     *
     * @param string $name
     *   The name of the state value.
     * @param mixed $default
     *   (Optional) default value to return if the value is not set.
     *
     * @return mixed
     *   The corresponding state value.
     */
    public function getState($name, $default = null)
    {
        return isset($this->state[$name]) ? $this->state[$name] : $default;
    }

    /**
     * Gets the base directory to use for relative paths.
     *
     * It can be
     *
     * @return string
     *   The working directory without trailing slash. ('' would represent
     *   root.)
     */
    protected function getBaseDirectory()
    {
        return $this->config['base_directory'] ?? getcwd();
    }

    /**
     * Validates a path. Logs error if validation fails.
     *
     * @param string $path
     *   Path; can be relative or absolute; can be file or directory.
     * @param bool $check_existence
     *   (Optional) If true, check existence of the path; otherwise check
     *   whether the 'dirname' of the path exists and is a directory. Return
     *   '' if not.
     *
     * @return string
     *   Validated absolute canonical path, or '' if the path is invalid.
     */
    public function validatePath($path, $check_existence = true)
    {
        if (!is_string($path) || !$path) {
            $this->getLogger()->error("Not a pathname: {path}.", ['path' => $path]);
            return '';
        }

        // Make absolute.
        if ($path[0] !== '/') {
            $path = $this->getBaseDirectory() . '/' . $path;
        }

        // Normalize: strip trailing slash, unless it's the root directory (which
        // is in line with low level functions like dirname().)
        if (substr($path, -1) === '/' && $path !== '/') {
            $path = rtrim($path, '/');
            if (file_exists($path) && !is_dir($path)) {
                $this->getLogger()->error("{path} ends with a directory separator but is not a directory.", ['path' => "$path/"]);
                return '';
            }
        }

        // We don't support processing symlinks (at least for now) because we
        // get the canonical path just below, and no reference to the original
        // link path would be left at all. Decision for now is, we might as well
        // skip them. (Our caller will likely consider this an error, unlike
        // processFileOrDir()... which is fine.)
        $canonicalize_full_path = $check_existence;
        if ($check_existence && is_link($path)) {
            if (empty($this->config['case_insensitive_filesystem'])) {
                $this->getLogger()->error("{path} is a symlink; this is not supported.", ['path' => $path]);
                return '';
            }
            // Don't canonicalize the symlink itself, only the base directory -
            // just like if $check_existence is false.
            $canonicalize_full_path = false;
        }

        // Check existence of the file... except if $check_existence is false,
        // but then the base directory must still exist and be a directory.
        $check_path = $check_existence ? $path : dirname($path);
        // file_exists will handle symlinks correctly; returns false if the
        // target file does not exist.
        if ($check_existence && !file_exists($check_path)) {
            $this->getLogger()->error("{path} does not exist.", ['path' => $check_path]);
            return '';
        }
        if (!$check_existence && !is_dir($check_path)) {
            $this->getLogger()->error("{path} is not a directory.", ['path' => $check_path]);
            return '';
        }

        // It's also possible that a path component of the file/dir is a
        // symlink, which we're fine with, but processing will be done on the
        // canonical name, not the original name as provided. This resolves
        // symlinks and parts named .. / .
        $check_path = $canonicalize_full_path ? $path : dirname($path);
        $realpath = realpath($check_path);
        if (!$realpath) {
            $this->getLogger()->error("Unexpected error: realpath({path}) returned nothing.", ['path' => $check_path]);
            return '';
        }
        if (!$canonicalize_full_path) {
            $realpath .= '/' . basename($path);
        }

        if ($realpath !== $path) {
            $this->getLogger()->debug('Processing {path} as {realpath}.', ['path' => $path, 'realpath' => $realpath]);
        }
        return $realpath;
    }

    /**
     * Do processing on file paths.
     *
     * The idea is that this method is the general entry point and is not
     * called recursively, so any pre- and post-processing could be done here
     * (in an overridden method in a child class).
     *
     * @param string[] $paths
     *   Paths; can be relative or absolute; can be files or directories.
     *
     * @return bool
     *   True if processing was actually done.
     */
    public function processPaths(array $paths)
    {
        // Validate all paths beforehand.
        $ok_paths = [];
        foreach ($paths as $path) {
            $path = $this->validatePath($path);
            if ($path) {
                // Silently deduplicate paths, by using value as key. Process
                // the first value (which can make a difference on case
                // insensitive file systems).
                $path_key = !empty($this->config['case_insensitive_filesystem']) ? strtolower($path) : $path;
                if (!isset($ok_paths[$path_key])) {
                    $ok_paths[$path_key] = $path;
                }
            } else {
                $this->state['errors']++;
            }
        }

        // Process paths only if all of them could be validated.
        $return = !(bool)$this->state['errors'];
        if (!$this->state['errors']) {
            foreach ($ok_paths as $path) {
                $this->processFileOrDir($path);
            }
        }

        return $return;
    }

    /**
     * Processes a path (file or directory). Must exist and be 'processable'.
     *
     * Child classes should only check things here that need to be checked for
     * every recursive path; other things (e.g. existence) should have been
     * checked already.
     *
     * @param $path
     *   Absolute path to process.
     */
    protected function processFileOrDir($path)
    {
        if (empty($this->config['process_symlinks']) && is_link($path)) {
            // Symlinks are considered errors in validatePath() but we may
            // encounter them while doing recursive processing, in which case
            // we skip them. (So the registered 'state' is different.)
            $this->getLogger()->error("{path} is a symlink; this is not supported.", ['path' => $path]);
            $this->state['symlinks_skipped']++;
        } elseif (is_dir($path)) {
            $this->processDirectory($path);
        } else {
            $this->processFile($path);
        }
    }

    /**
     * Processes (the files inside) a directory. Must exist and be
     * 'processable'.
     *
     * This has its own method so child classes can do pre/post processing per
     * directory - or process a directory at once rather than calling
     * processFileOrDir() recursively. For per-directory processing that needs
     * to be done after the entries in the directory are read but before any of
     * them are processed, readDirectory() can be extended.
     *
     * On case insensitive file systems, the part that was a parameter to
     * processPath() (and therefore not recursively read from the filesystem by
     * readDirectory()) may be cased differently than the actual directory.
     *
     * @param $directory
     *   Directory to process; pathname must be absolute.
     */
    protected function processDirectory($directory)
    {
        $relative_paths = $this->readDirectory($directory);
        foreach ($relative_paths as $relative_path) {
            // Our processing is 'mostly depth-first': if our entry is a
            // directory, we finish the whole tree before returning here. But
            // inside a directory we don't process directories before files.
            $this->processFileOrDir("$directory/$relative_path");
        }
    }

    /**
     * Reads a directory. Must exist and be 'processable'.
     *
     * @param string $directory
     *   The directory to read, as an absolute path. (We assume it's readable.)
     *
     * @return string[]
     *   The filenames in this directory.
     */
    protected function readDirectory($directory)
    {
        $exclude = ['.', '..'];

        $paths = [];
        if ($dh = opendir($directory)) {
            while (($pathname = readdir($dh)) !== false) {
                if (!in_array($pathname, $exclude)) {
                    $paths[] = $pathname;
                }
            }
            closedir($dh);
        }

        if (!empty($this->config['sort_directory_entries'])) {
            sort($paths);
        }

        return $paths;
    }

    /**
     * Process one file.
     *
     * On case insensitive file systems, the part that was a parameter to
     * processPath() (and therefore not recursively read from the filesystem by
     * readDirectory()) may be differently cased from the actual dir/file.
     *
     * @param string $filename
     *   The file to process, as an absolute path. (We assume it's readable.)
     */
    protected function processFile($filename)
    {
    }
}
