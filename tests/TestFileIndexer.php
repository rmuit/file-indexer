<?php

namespace Wyz\PathProcessor\Tests;

use Wyz\PathProcessor\FileIndexer;

/**
 * Helper class to test FileIndexer; contains some extra checks.
 */
class TestFileIndexer extends FileIndexer
{
    protected function processDirectory($directory)
    {
        $dir_cache_key = $this->modifyCacheKey($this->getPathRelativeToAllowedBase($directory));

        // The number of database records cached in this class must be equal
        // before/after all the (recursive) processing we do...
        $before = $this->countTwoDimensionalArray($this->recordsCache);
        // ...except if there is a file record named after this directory
        // (which clearly is a wrong record); that chould be deleted afterwards.
        list($parent_dir, $wrong_file) = $this->splitFileName($dir_cache_key);
        if (isset($this->recordsCache[$parent_dir][$wrong_file]) && $this->getConfig('remove_nonexistent_from_index')) {
            $before--;
        }

        $subdirs_before = $this->countTwoDimensionalArray($this->subdirsCache);

        parent::processDirectory($directory);

        // Check that any records that would have been added, are deleted again.
        // (We could count only the items for $directory specifically, but want
        // to be as strict as we can.)
        $after = $this->countTwoDimensionalArray($this->recordsCache);
        if ($before !== $after) {
            throw new \Exception("Cached items still left after processing '$directory': before $before, after $after.");
        }

        $subdirs_after = $this->countTwoDimensionalArray($this->subdirsCache);
        if ($subdirs_before !== $subdirs_after) {
            throw new \Exception("Cached subdirs still left after processing '$directory': before $before, after $after.");
        }
    }

    protected function processFile($filename)
    {
        // Copied from parent:
        $relative_path = $this->getPathRelativeToAllowedBase($filename);
        $key_file = $this->modifyCacheKey($relative_path);
        list($key_dir) = $this->splitFileName($key_file);
        // One entry can be added to the cache, only if the cache for this
        // directory is already set. (If not, adding the entry could lead to
        // bugs; see the code.)
        $can_add_file_to_cache = isset($this->recordsCache[$key_dir]);

        $before = $this->countTwoDimensionalArray($this->recordsCache);

        parent::processFile($filename);

        // Check.
        $after = $this->countTwoDimensionalArray($this->recordsCache);
        if ($after !== $before && (!$can_add_file_to_cache || $after !== $before + 1)) {
            throw new \Exception("Cached items still left after processing '$filename': before $before, after $after.");
        }
    }


    /**
     * Returns a count of all individual items in a two-dimensional array.
     *
     * @param array|null $two_dimensional_array
     *   The array to count. Can also be null.
     *
     * @return int
     *   The total number of file items, in this dir/file array.
     */
    protected function countTwoDimensionalArray($two_dimensional_array) {
        $count = 0;
        if (isset($two_dimensional_array)) {
            foreach ($two_dimensional_array as $cached_dir) {
                $count += count($cached_dir);
            }
        }

        return $count;
    }
}
