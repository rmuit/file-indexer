<?php

namespace Wyz\PathProcessor;

use Psr\Log\LoggerInterface;

/**
 * Removes files or directories, recursively.
 *
 * While iterating through directories, it first reads the contents from the
 * directory before unlink()ing each one individually. This may not be the most
 * efficient (could we 'rm * in another way?), but it's simple reuse of code.
 */
class PathRemover extends PathProcessor
{
    public function __construct(LoggerInterface $logger, array $config = [])
    {
        // By default, process symlinks by unlinking them just like files.
        parent::__construct($logger, $config + ['process_symlinks' => true]);
    }

    public function processPaths(array $paths)
    {
        $processed = parent::processPaths($paths);

        // We assume that if processing was canceled, an error was logged.
        if ($processed) {
            $value = $this->getState('errors');
            if ($value) {
                // We assume we've logged already and this is not an actionable
                // message so 'warning' should be enough.
                $this->getLogger()->warning('Encountered {count} error(s).', ['count' => $value]);
            }
        }

        return $processed;
    }

    protected function processDirectory($directory)
    {
        parent::processDirectory($directory);

        // We could only do this if no files had errors, but we're lasy.
        if (!rmdir($directory)) {
            $this->getLogger()->error('Could not remove directory {dir}.', ['dir' => $directory]);
            $this->state['errors']++;
        }
    }

    protected function processFile($filename)
    {
        parent::processFile($filename);

        if (!unlink($filename)) {
            $this->getLogger()->error('Could not remove file {file}.', ['file' => $filename]);
            $this->state['errors']++;
        }
    }
}
