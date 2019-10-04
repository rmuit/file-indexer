<?php

namespace Wyz\PathProcessor;

use Psr\Log\LoggerInterface;
use RuntimeException;

/**
 * PathProcessor which only processes files within a certain base directory.
 *
 * This is split out from (extends) PathProcessor to achieve a bit of code
 * separation, though introducing many layers of inheritance arguably doesn't
 * have any other uses.
 *
 * Note that relative path names passed into processPaths() must be
 * relative to getBaseDirectory() (which is the working directory or the
 * 'base_directory' setting), which is not necessarily relative to the
 * 'allowed_base_directory' setting. (This class does not set 'base_directory',
 * in order to not change behavior for relative paths.)
 *
 * The 'allowed_base_directory' config value must be set.
 */
class SubpathProcessor extends PathProcessor
{
    /**
     * SubpathProcessor constructor.
     *
     * @param \Psr\Log\LoggerInterface $logger
     *   A logger instance.
     * @param array $config
     *   (Optional) configuration values.
     *
     * @throws \RuntimeException
     *   If configuration values are incompatible.
     */
    public function __construct(LoggerInterface $logger, array $config = [])
    {
        if (empty($config['allowed_base_directory']) || !is_string($config['allowed_base_directory'])) {
            throw new RuntimeException("The 'allowed_base_directory' configuration value must be a non-empty string.");
        }
        parent::__construct($logger, $config);
    }

    /**
     * Returns the base directory which must contain any processed paths.
     *
     * @return string
     *
     * @throws \RuntimeException
     *   If the allowed_base_directory value was not configured in the constructor.
     */
    protected function getAllowedBaseDirectory()
    {
        return $this->config['allowed_base_directory'];
    }

    /**
     * {@inheritDoc}
     */
    public function validatePath($path, $check_existence = true)
    {
        $realpath = parent::validatePath($path, $check_existence);
        if ($realpath) {
            if (!$this->getPathRelativeToAllowedBase($realpath) && $realpath !== $this->getAllowedBaseDirectory()) {
                return '';
            }
        }

        return $realpath;
    }

    /**
     * Returns a relative version of a path, without otherwise validating it.
     *
     * @param string
     *   Path; existence will not be checked.
     *
     * @return string
     *   Path relative to the base dir; empty string can mean both 'the base
     *   dir itself' and 'invalid path' which the caller must take into account.
     */
    public function getPathRelativeToAllowedBase($path)
    {
        $base = $this->getAllowedBaseDirectory();
        // If $path is equal to the base directory, we'll return empty string
        // without logging anything, because the path is allowed.
        if ($path !== $base) {
            $base .= '/';
            if ((empty($this->config['case_insensitive_filesystem'])
                    ? strpos($path, $base) : stripos($path, $base)) !== 0) {
                $this->getLogger()->error("'{path}' is not inside an allowed base directory.", ['path' => $path]);
                return '';
            }
        }

        return substr($path, strlen($base));
    }
}
