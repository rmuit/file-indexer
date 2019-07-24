<?php

namespace Wyz\PathProcessor\Tests;

use PDO;
use Psr\Log\Test\TestLogger;
use RuntimeException;
use PHPUnit\Framework\TestCase;
use Wyz\PathProcessor\FileIndexer;
use Wyz\PathProcessor\PathRemover;

// @todo use in-memory SQLite after we're done setting up tests.
// @todo document the env vars which are overridable (by env or xml) <- document interpreter option -d variables_order=EGPCS ?

/**
 * Test class for FileIndexer.
 *
 * This contains tests for checking behavior of indexing files in case
 * sensitive and insensitive directories. The directories can be specified in
 * environment variables TEST_DIR_CASE_SENSITIVE and TEST_DIR_CASE_INSENSITIVE
 * (both defaulting to a subdirectory of /tmp). If the sensitive-ness of the
 * directory is not as expected, tests are skipped.
 *
 * This test has dependencies on a file system, and a database for storing
 * indexed values. The database is not abstracted out of the test; SQLite is
 * used (in-memory, unless environment variable FILE_INDEXER_TEST_DB_FILE is
 * nonempty).
 *
 * My approach to writing tests has been to not make separate tests for every
 * single method, but instead to devise a test file system structure that will
 * hopefully run through all code I feel needs to be checked.
 *
 * isInteractiveSession() / confirm() is not tested, because there is very
 * little
 *
 * There are no separate tests for PathProcessor/SubpathProcessor; the proper
 * working of recursive processing, relative/absolute paths, skipping symlinks,
 * should be implicitly tested by this class. Other details like exact
 * logs/exceptions are considered unnecessary to test.
 *
 *
 *
 * @todo say something about skipping tests for case (in)sensitive FS. and
 *     maybe getting it to run
 *   (and for people to tell me if they have better ideas about configuring the
 *     dirs)
 */
class FileIndexerTest extends TestCase
{

    /**
     * Test database connection.
     *
     * We set this up once and use it in all our tests.
     *
     * @var PDO
     */
    protected $pdo_connection;

    /**
     * Signifies whether to skip tests on a case sensitive file system.
     *
     * Contains TRUE if we should not try to run tests on a dir with this
     * casing. This acts as a static value that is set once by an initial test
     * so subsequent tests don't need to repeat creating directories and
     * failing.
     *
     * The actual static-ness of this value is not ideal but we want to reset
     * it in static setUpBeforeClass(). We'll assume no two test classes ever
     * run simultaneously.
     *
     * @var bool
     */
    protected static $skipCaseSensitiveTests;

    /**
     * Signifies whether to skip tests on a case insensitive file system.
     *
     * @var bool
     */
    protected static $skipCaseInsensitiveTests;

    public static function setUpBeforeClass()
    {
        self::$skipCaseInsensitiveTests = self::$skipCaseSensitiveTests = false;
        parent::setUpBeforeClass();
    }

    /**
     * Creates a directory which is supposed to be case sensitive.
     *
     * It should not exist yet; This method creates it and the test should
     * remove it at the end. The base directory can be set in an environment
     * variable (which means in phpcs.xml).
     *
     * @todo remove the directory at the end of any test, or all tests (that's
     *   even better but I'm not sure if that is possible)
     *
     * @return string
     *   Directory name if it is case sensitive; empty string if we don't have
     *   a case sensitive directory to run tests on.
     *
     * @throws \RuntimeException
     *   The directory cannot be created.
     */
    protected function createCaseSensitiveDir()
    {
        if (self::$skipCaseSensitiveTests) {
            return '';
        }

        $base_dir = !empty($_ENV['TEST_DIR_CASE_SENSITIVE']) ? $_ENV['TEST_DIR_CASE_SENSITIVE'] : '/tmp';

        // There's no PHP native call to create a temporary directory, so we'll
        // first create a file, then remove it and quickly replace it by a
        // directory, assuming that the name won't get reused.
        $tmp_file = tempnam($base_dir, 'fileindexertest');
        $path_components = explode(DIRECTORY_SEPARATOR, $tmp_file);
        $filename = array_pop($path_components);
        array_push($path_components, ucfirst($filename));
        $tmp_file_miscased = implode(DIRECTORY_SEPARATOR, $path_components);
        // But first, try to check the file, also with a differently cased name.
        // This should tell us whether the directory is case sensitive.
        $stat = file_exists($tmp_file);
        if (!$stat) {
            throw new RuntimeException("Cannot stat temporary file $tmp_file; tests cannot run.");
        }
        $stat = file_exists($tmp_file_miscased);
        unlink($tmp_file);
        if ($stat) {
            // The directory is NOT case sensitive.
            self::$skipCaseSensitiveTests = true;
            $tmp_file = '';
        } else {
            if (!mkdir($tmp_file)) {
                throw new RuntimeException("Cannot create temporary directory $tmp_file; tests cannot run.");
            }
        }

        return $tmp_file;
    }

    /**
     * Creates a directory which is supposed to be case insensitive.
     *
     * It should not exist yet; This method creates it and the test should
     * remove it at the end. The base directory can be set in an environment
     * variable (which means in phpcs.xml).
     *
     * @todo remove the directory at the end of any test, or all tests (that's
     *   even better but I'm not sure if that is possible)
     *
     * @return string
     *   Directory name if it is case sensitive; empty string if we don't have
     *   a case sensitive directory to run tests on.
     *
     * @throws \RuntimeException
     *   The directory cannot be created.
     */
    protected function createCaseInsensitiveDir()
    {
        if (self::$skipCaseInsensitiveTests) {
            return '';
        }

        $base_dir = !empty($_ENV['TEST_DIR_CASE_SENSITIVE']) ? $_ENV['TEST_DIR_CASE_SENSITIVE'] : '/tmp';

        // There's no PHP native call to create a temporary directory, so we'll
        // first create a file, then remove it and quickly replace it by a
        // directory, assuming that the name won't get reused.
        $tmp_file = tempnam($base_dir, 'fileindexertest');
        $path_components = explode(DIRECTORY_SEPARATOR, $tmp_file);
        $filename = array_pop($path_components);
        array_push($path_components, ucfirst($filename));
        $tmp_file_miscased = implode(DIRECTORY_SEPARATOR, $path_components);
        // But first, try to stat the file, also with a differently cased name.
        // This should tell us whether the directory is case sensitive.
        $stat = file_exists($tmp_file);
        if (!$stat) {
            throw new RuntimeException("Cannot stat temporary file $tmp_file; tests cannot run.");
        }
        $stat = file_exists($tmp_file_miscased);
        unlink($tmp_file);
        if (!$stat) {
            // The directory is NOT case insensitive.
            self::$skipCaseInsensitiveTests = true;
            $tmp_file = '';
        } else {
            if (!mkdir($tmp_file)) {
                throw new RuntimeException("Cannot create temporary directory $tmp_file; tests cannot run.");
            }
        }

        return $tmp_file;
    }


    /**
     * Create a database / table(s).
     *
     * If the database already exists, table is dropped and recreated.
     *
     * @param bool $case_insensitive
     *   True if the dir/filename columns should be case insensitive.
     * @param bool $for_case_insensitive_fs
     *   True if the file system it is going to be used for, is also case
     *   insensitive.
     */
    protected function createDatabase($case_insensitive, $for_case_insensitive_fs)
    {
        // Set up an SQLite database for testing, once before running all tests.
        // This is usually an in-memory database; we don't remove the file
        // afterwards. (Though we've seen 'sqlite:memory:' create actual
        // databases on the file system, so apparently it doesn't always work.)
        // @todo but then subsequent tests will fail, right? I guess file DBs don't work yet.
        if (empty($this->pdo_connection)) {
            if (!empty($_ENV['FILE_INDEXER_TEST_DB_FILE'])) {
                $this->pdo_connection = new PDO('sqlite:' . tempnam('/tmp', 'fileindexertestdb'));
            } else {
                $this->pdo_connection = new PDO('sqlite:memory:');
            }
        }

        // Even 'sqlite:memory:' databases can get preserved over separate test
        // runs (i.e. also if $this->pdo_connection did not exist yet), so
        // always try to drop the table.
        $ret = $this->pdo_connection->exec('DROP TABLE file');
        $sensitivity = $case_insensitive ? ' COLLATE NOCASE' : '';
        $ret = $this->pdo_connection->exec("CREATE TABLE IF NOT EXISTS file (
          fid            INTEGER PRIMARY KEY,
          dir            TEXT    NOT NULL$sensitivity,
          filename       TEXT    NOT NULL$sensitivity,
          sha1           TEXT    NOT NULL,
          UNIQUE (dir, filename) ON CONFLICT ABORT);");
        $ret = $this->pdo_connection->exec('CREATE INDEX sha1 ON file (sha1)');

        // In SQLite we need to set case sensitive behavior of LIKE
        // globally (which is off by default apparently).
        if (!$case_insensitive && !$for_case_insensitive_fs) {
            $ret = $this->pdo_connection->exec('PRAGMA case_sensitive_like=ON');
        } else {
            // Better be sure it didn't stay case sensitive from last time.
            $ret = $this->pdo_connection->exec('PRAGMA case_sensitive_like=OFF');
        }
    }

    /**
     * Fetches contents of the database table.
     *
     * @return array[]
     *   Array of numerically indexed records containing 'dir', 'filename' and
     *   'sha1' keys.
     */
    protected function getDatabaseContents()
    {
        $statement = $this->pdo_connection->prepare('SELECT dir,filename,sha1 FROM file');
        if (!$statement) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        $ret = $statement->execute();
        if (!$ret) {
            // This is very unexpected; no details logged so far.
            throw new RuntimeException('Database statement execution failed.');
        }
        return $statement->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Asserts that the database contents are equal to an array, after sorting.
     *
     * @param array $expected
     *   The expected value, except keys are 0,1,2 instead of dir/filename/sha,
     *   for easier input / reading.
     */
    protected function assertDatabaseContents(array $expected)
    {
        $database_contents = $this->getDatabaseContents();
        sort($database_contents);
        $compare = [];
        foreach ($expected as $value) {
            $this->assertIsArray($value);
            $this->assertEquals(3, count($value), 'Sub-array for database contents comparison has wrong amount of elements.');
            $compare[] = ['dir' => $value[0], 'filename' => $value[1], 'sha1' => $value[2]];
        }
        sort($compare);
        $this->assertEquals($compare, $database_contents);
    }

    /**
     * Asserts that messages logged are equal to a given array.
     *
     * @param array $expected
     *   Expected log messages in the form of "LEVEL: message"
     * @param TestLogger $logger
     *   A logger instance which contains the logged messages.
     */
    protected function assertLogs(array $expected, TestLogger $logger)
    {
        $logs = [];
        foreach ($logger->records as $record) {
            $message = $record['message'];
            foreach ($record['context'] as $key => $value) {
                $message = str_replace('{' . $key. '}', $value, $message);
            }
            $logs[] = "{$record['level']}: $message";
        }
        $this->assertEquals($expected, $logs);
    }

    /**
     * Quick helper function: reindexes stuff, compares database and logs.
     */
    public function indexAndAssert(FileIndexer $indexer, array $process_paths, array $expected_database_contents, array $expected_logs)
    {
        /** @var TestLogger $logger */
        $logger = $indexer->getLogger();
        $logger->records = [];
        $indexer->processPaths($process_paths);
        $this->assertLogs($expected_logs, $logger);
        $this->assertDatabaseContents($expected_database_contents);
    }

    /**
     * Creates some files for doing tests on a case sensitive file system.
     *
     * The callers assume knowledge about the created file structure, so we
     * can't just change it. It's just split into its own function for reuse.
     *
     * @return string
     *   Base directory name if all files were created. Empty string if
     *   creation was not attempted.
     *
     * @throws \RuntimeException
     *   Not all files could be created.
     */
    protected function createCaseSensitiveFileStructure() {
        $dir = $this->createCaseSensitiveDir();
        if (!$dir) {
            return '';
        }

        // Create:
        // - AA  (empty file)
        // - AB  (empty file)
        // - aa/BB: symlink to bb/cc/AA
        // - aa/bb/cc/AA
        // - aa/bb/cc/aa
        // Indexing "AA" and "aa" at the same time on a case sensitive FS tests
        // - whether empty files are indexed
        // - whether indexing multiple files in one call to processPaths works
        // - whether (re)indexing multiple individual files in the same dir, in
        //   one call to processPaths, works.
        // - whether recursive indexing of a directory works, also when a
        //   directory contains only subdirectories (containing files)
        // - whether all the files go into the database
        // - whether the symlink is skipped
        // @todo check the above description: how true is it? How true is it with insensitive db?
        //   Should we move this to the tests or not?
        foreach (['AA', 'AB'] as $file) {
            $fp = fopen("$dir/$file", 'w');
        }
        fclose($fp);
        foreach (['aa', 'aa/bb', 'aa/bb/cc'] as $subdir) {
            mkdir("$dir/$subdir");
        }
        $fp = fopen("$dir/aa/bb/cc/AA", 'w');
        fwrite($fp, "hi");
        fclose($fp);
        $fp = fopen("$dir/aa/bb/cc/aa", 'w');
        fwrite($fp, "hello world");
        fclose($fp);
        // We might continue if the file system can't handle symlinks because
        // it's only going to report an error anyway, not test functionality.
        // But is it worth it? Are there still non-symlink-supporting FSes?
        symlink('bb/cc/AA', "$dir/aa/BB");

        return $dir;
    }

    /**
     * Creates some files for doing tests on a case insensitive file system.
     *
     * The callers assume knowledge about the created file structure, so we
     * can't just change it. It's just split into its own function for reuse.
     *
     * @return string
     *   Base directory name if all files were created. Empty string if
     *   creation was not attempted.
     *
     * @throws \RuntimeException
     *   Not all files could be created.
     */
    protected function createCaseInsensitiveFileStructure() {
        $dir = $this->createCaseInSensitiveDir();
        if (!$dir) {
            return '';
        }

        // Create:
        // NOT     - AA  (empty file)      @TODO remove the 'NOT' lines
        // - AB  (empty file)
        // - aa/BX: symlink to bb/cc/AA
        // - aa/bb/cc/AA
        //   NOT      - aa/bb/cc/aa
        $fp = fopen("$dir/AB", 'w');
        fclose($fp);
        foreach (['aa', 'aa/bb', 'aa/bb/cc'] as $subdir) {
            mkdir("$dir/$subdir");
        }
        $fp = fopen("$dir/aa/bb/cc/AA", 'w');
        fwrite($fp, "hi");
        fclose($fp);
        // We might continue if the file system can't handle symlinks because
        // it's only going to report an error anyway, not test functionality.
        // But is it worth it? Are there still non-symlink-supporting FSes?
        symlink('bb/cc/AA', "$dir/aa/BX");

        return $dir;
    }

    // @todo do processing of relative and absolute directory names as input, in different tests
    // todo also when setting base dir. <- even when feeding absolute paths; in that case we should be able to set it to a wrong value.

    /**
     * Test case sensitive file system with case sensitive database.
     *
     * @todo check if we should split out various tests into their own functions (we just need to do our own initialization).
     */
    public function testSfileSdb()
    {
        // Note below tests assume knowledge about the created file structure.
        $base_dir = $this->createCaseSensitiveFileStructure();
        if (!$base_dir) {
            $this->markTestSkipped('Case sensitive directory is not configured or not actually case sensitive.');
            return;
        }

        $this->createDatabase(false, false);

        $logger = new TestLogger();
        $indexer_default_config = [
            'pdo' => $this->pdo_connection,
            'allowed_base_directory' => $base_dir,
            'case_insensitive_database' => false,
        ];
        $indexer = new FileIndexer($logger, $indexer_default_config);
        $indexer_reindex = new FileIndexer($logger, $indexer_default_config + ['reindex_all' => true]);
        $indexer_remove = new FileIndexer($logger, $indexer_default_config + ['remove_nonexistent_from_index' => true]);

        $database_contents = [
            ['', 'AA', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['', 'AB', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['aa/bb/cc', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi
            ['aa/bb/cc', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'], // hello world
        ];
        // For increased testing coverage, pass separate path names into the
        // method. Recursive processing will be tested alongside processing of
        // separate paths, because aa contains subdirs.
        $this->indexAndAssert($indexer, ["$base_dir/AA", "$base_dir/AB", "$base_dir/aa"], $database_contents, [
            "error: $base_dir/aa/BB is a symlink; this is not supported.",
            "info: Added 4 new file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // Reindex the same directory, to see if anything changes now that the
        // database has contents. (This also tests if files in the 'root'
        // directory can be read back from the DB; conceivably, a DB system
        // could mess up empty string vs. null.)
        $this->indexAndAssert($indexer, ["$base_dir/AA", "$base_dir/AB", "$base_dir/aa"], $database_contents, [
            // Errors logged by the base class contain full paths; others don't.
            "error: $base_dir/aa/BB is a symlink; this is not supported.",
            "info: Skipped 4 already indexed file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // And do the same again, to see if we can specify the base dir.
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            "error: $base_dir/aa/BB is a symlink; this is not supported.",
            "info: Skipped 4 already indexed file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // Remove the symlink; it will just cause noise in logs from now on.
        unlink("$base_dir/aa/BB");

        // Change a file and then reindex it. (There is no difference here in
        // whether it's a singular file or part of a directory; all that logic
        // is in processFile().)
        copy("$base_dir/aa/bb/cc/AA", "$base_dir/AB");
        // This will not change anything.
        $this->indexAndAssert($indexer, ["$base_dir/AB"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // We need to pass 'reindex_all' for this to have effect. Also verify
        // message for 'equal' files.
        $database_contents[1][2] = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'; // hi
        $this->indexAndAssert($indexer_reindex, ["$base_dir/AA", "$base_dir/AB"], $database_contents, [
            "info: Updated 1 file(s).",
            "info: Reindexed 1 file(s) which were already indexed and equal.",
        ]);

        // Move a file around. This is the same as removing and adding a file;
        // FileIndexer doesn't have rename detection. With a case sensitive
        // file system, re-casing a file should 'move' it - and in combination
        // with a case sensitive database, this should be reflected in the db.
        // (Also implicitly test: the directory which is stored, should not be
        // influenced by the value passed to processPaths() / starting from a
        // subdirectory. Though that's obvious.)
        rename("$base_dir/aa/bb/cc/AA", "$base_dir/aa/bb/cc/Aa");
        $database_contents[] = ['aa/bb/cc', 'Aa', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        // File should be added as extra file; existing record is not removed
        // but a warning is logged (checkIndexedRecordsNonexistentInDir()).
        $this->indexAndAssert($indexer, ["$base_dir/aa/bb"], $database_contents, [
            "warning: Indexed records exist for the following nonexistent files in directory 'aa/bb/cc': AA.",
            "info: Added 1 new file(s).",
            // Only 'aa' gets found and skipped.
            "info: Skipped 1 already indexed file(s).",
        ]);
        unset($database_contents[2]);
        // Remove 'AA' now.
        $this->indexAndAssert($indexer_remove, ["$base_dir/aa/bb"], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory 'aa/bb/cc': AA.",
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Test warning/removal of records in subdirectories that don't exist
        // anymore. (checkIndexedRecordsInNonexistentSubdirs().)
        // NOTE - this is one of two sub-tests which needs the 'subdirsCache'
        // property populated. So we're implicitly testing the SQL query which
        // populates that, but not when we're querying the complete $base_dir;
        // we do that in the checkIndexedRecordsInNonexistentDir() test.
        // Just move the directory to another file.
        rename("$base_dir/aa/bb/cc", "$base_dir/cc");
        // 1a: See if the query still picks up the records which are now in a
        //     two layers deep missing directory.
        $this->indexAndAssert($indexer, ["$base_dir/aa"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory 'aa/bb': cc.",
        ]);
        // 1b: Remove the now-empty bb; see if the query still picks up the
        //     records which are now in a two layers deep missing directory.
        rmdir("$base_dir/aa/bb");
        $this->indexAndAssert($indexer, ["$base_dir/aa"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory 'aa': bb.",
        ]);
        // 2a: Test that the 2 records for aa/bb(/cc) are removed.
        unset($database_contents[3]);
        unset($database_contents[4]);
        $this->indexAndAssert($indexer_remove, ["$base_dir/aa"], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'aa/bb'.",
        ]);
        // Recap: we only have two indexed files named AA and AB, and we have
        // two files in cc/ which are not indexed yet. For 2b, we first need to
        // index them.
        $database_contents[2] = ['cc', 'Aa', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $database_contents[3] = ['cc', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed']; // hello world
        $this->indexAndAssert($indexer_remove, ["$base_dir/cc"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);
        // 2b: Test again that the 2 records for cc are removed. The difference
        //     with 2a is that the SQL query operates on a directory that is
        //     directly in the root directory. (This difference is basically
        //     encoded by concatenateRelativePath(), and as long as that stays
        //     the same, we only need to do this test variation once, not in
        //     all tests which use the same LIKE construct for deletions.)
        rename("$base_dir/cc", "$base_dir/aa/cc");
        // We can't just index 'cc' because we'll get a "not found". So reindex
        // the whole base directory.
        $database_contents[2][0] = $database_contents[3][0] = 'aa/cc';
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'cc'.",
            "info: Added 2 new file(s).",
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name. (checkIndexedRecordWithSameNameAsDir().)
        unlink("$base_dir/AA");
        rename("$base_dir/aa/cc", "$base_dir/AA");
        // Let's reinitialize to see where we are after all the above changes.
        $database_contents = [
            ['', 'AA', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty. Does not exist anymore.
            ['', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi
            ['AA', 'Aa', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - this + next will now be reindexed.
            ['AA', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'], // hello world
            ['aa/cc', 'Aa', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - this + next does not exist anymore.
            ['aa/cc', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'], // hello world
        ];
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            "warning: Indexed record exists for file 'AA', which actually matches a directory.",
            // This one we expect because of the previous test, but we already tested:
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory 'aa': cc.",
            "info: Added 2 new file(s).",
            "info: Skipped 1 already indexed file(s).",
        ]);
        // Remove the duplicate indexed item for nonexistent file.
        unset($database_contents[0]);
        unset($database_contents[4]);
        unset($database_contents[5]);
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed indexed record for file 'AA' which actually matches a directory.",
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'aa/cc'.",
            "info: Skipped 3 already indexed file(s).",
        ]);

        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name. (checkIndexedRecordsInNonexistentDir().)
        // First move the directory out of the way; easier than removing.
        rename("$base_dir/AA", "$base_dir/cc");
        rename("$base_dir/AB", "$base_dir/AA");
        // Reinitialize again
        $database_contents = [
            ['', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi
            ['', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - moved to AA now.
            ['AA', 'Aa', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - moved now.
            ['AA', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'], // hello world
            ['cc', 'Aa', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - this + next will now be reindexed.
            ['cc', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'], // hello world
        ];
        // This sub-test needs subdirsCache filled, so we'll only find these
        // records if we reindex the parent. NOTE - as mentioned above, this
        // test also needs to index / find / remove stuff from $base_dir,
        // because that properly tests the SQL query which fills subdirsCache.
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            "warning: Indexed records exist for the following nonexistent files in directory '': AB.",
            "warning: Indexed records exist with 'AA' (which is a file) as nonexistent base directory.",
            "info: Added 3 new file(s).",
        ]);
        // Remove the duplicate indexed items for nonexistent files.
        unset($database_contents[1]);
        unset($database_contents[2]);
        unset($database_contents[3]);
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory '': AB.",
            "info: Removed 2 indexed record(s) with 'AA' (which is a file) as nonexistent base directory.",
            "info: Skipped 3 already indexed file(s).",
        ]);

        // Remove the directory after the test.
        $processor = new PathRemover($logger);
        $processor->processPaths([$base_dir]);
    }

    /**
     * Test case sensitive file system with case insensitive database.
     *
     * The first part doesn't just test but also outlines how we are able to
     * confuse the database by indexing separate arguments with different case.
     */
    public function testSfileIdb()
    {
        // Note below tests assume knowledge about the created file structure.
        $base_dir = $this->createCaseSensitiveFileStructure();
        if (!$base_dir) {
            $this->markTestSkipped('Case sensitive directory is not configured or not actually case sensitive.');
            return;
        }

        $this->createDatabase(true, false);

        $logger = new TestLogger();
        $indexer_default_config = [
            'pdo' => $this->pdo_connection,
            'allowed_base_directory' => $base_dir,
        ];
        $indexer = new FileIndexer($logger, $indexer_default_config);
        $indexer_reindex = new FileIndexer($logger, $indexer_default_config + ['reindex_all' => true]);
        $indexer_remove = new FileIndexer($logger, $indexer_default_config + ['remove_nonexistent_from_index' => true]);

        // aa/BB is a symlink and won't be processed. aa/bb won't be processed
        // because it's got the same casing and aa/bb comes first. Depends on:
        // - 'collation' of directory listing, BB before bb
        // - the fact that the code decides not to process bb while reading the
        //   directory, before concluding BB won't be processed. (Changing this
        //   would require more code changes than we want to make.)
        $database_contents = [
            ['', 'AA', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['', 'AB', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
        ];
        // For increased testing coverage, pass separate path names into the
        // method. Recursive processing will be tested alongside processing of
        // separate paths, because aa contains subdirs. AA and aa both will be
        // processed (even though that will cause strangeness further below)
        // because they are passed as separate arguments.
        $this->indexAndAssert($indexer, ["$base_dir/AA", "$base_dir/AB", "$base_dir/aa"], $database_contents, [
            "warning: Directory 'aa' contains entries for both BB and bb; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "error: $base_dir/aa/BB is a symlink; this is not supported.",
            "info: Added 2 new file(s).",
            "info: Skipped 1 symlink(s).",
        ]);

        unlink("$base_dir/aa/BB");
        // Reindex the same directory, to see if anything changes now that the
        // database has contents. (This also tests if files in the 'root'
        // directory can be read back from the DB; conceivably, a DB system
        // could mess up empty string vs. null.) This newly indexes aa/bb.
        $database_contents[] = ['aa/bb/cc', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->indexAndAssert($indexer, ["$base_dir/AA", "$base_dir/AB", "$base_dir/aa"], $database_contents, [
            "warning: Directory 'aa/bb/cc' contains entries for both AA and aa; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "info: Added 1 new file(s).",
            "info: Skipped 2 already indexed file(s).",
        ]);
        // Now reindex again while specifying the base dir. Because we were
        // able to index both 'AA' and 'aa' above, now when we process them
        // 'aa' is skipped so the indexed entries inside of aa/ are considered
        // nonexisting. (In other words, the 2nd/3rd warning is not really true,
        // because of the first warning.) We'll leave it like that. We could
        // support this (2 entries on case sensitive filesystem and case
        // sensitive db) where one is a directory and one is a file, because
        // their data ends up in different columns. But we don't want to adjust
        // the code for that. People should just not do this, and use case
        // sensitive db instead.
        //   @todo maybe try and support it anyway, later? <= in that case, note it in the class doc.
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            "warning: Directory '' contains entries for both AA and aa; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "warning: Indexed records exist with 'AA' (which is a file) as nonexistent base directory.",
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Test / make clear what happens when we process both AA and aa. We
        // don't necessarily want this but we can do this by passing them as
        // separate files. Not reindexing will do nothing.
        $this->indexAndAssert($indexer, ["$base_dir/aa/bb/cc/AA", "$base_dir/aa/bb/cc/aa"], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);
        // Reindexing will first do 'nothing' and then update the file. (The
        // log messages for aa & AA are reversed but that's just the way
        // FileIndexer logs.)
        $database_contents[2] = ['aa/bb/cc', 'aa', '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'];
        $this->indexAndAssert($indexer_reindex, ["$base_dir/aa/bb/cc/AA", "$base_dir/aa/bb/cc/aa"], $database_contents, [
            "info: Updated 1 file(s).",
            "info: Reindexed 1 file(s) which were already indexed and equal."
        ]);
        // Updating again in the same order updates one and then the other,
        // and ends up the same.
        $this->indexAndAssert($indexer_reindex, ["$base_dir/aa/bb/cc/AA", "$base_dir/aa/bb/cc/aa"], $database_contents, [
            "info: Updated 2 file(s).",
        ]);
        // Reindexing the whole directory. This will use only AA again.
        $database_contents[2] = ['aa/bb/cc', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->indexAndAssert($indexer_reindex, ["$base_dir/aa/bb/cc"], $database_contents, [
            "warning: Directory 'aa/bb/cc' contains entries for both AA and aa; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "info: Updated 1 file(s).",
        ]);

        // This is the end of "what happens with 2 files with different case"
        // tests; get rid of aa so we don't have to deal with warnings anymore.
        // (We still have file /AA vs dir /aa; maybe remove later.)
        unlink("$base_dir/aa/bb/cc/aa");

        // Change a file's contents and then reindex it. (There is no
        // difference here in whether it's a singular file or part of a
        // directory; all that logic is in processFile().)
        copy("$base_dir/aa/bb/cc/AA", "$base_dir/AB");
        // This will not change anything.
        $this->indexAndAssert($indexer, ["$base_dir/AB"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // We need to pass 'reindex_all' for this to have effect. Also verify
        // message for 'equal' files.
        $database_contents[1][2] = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'; // hi
        $this->indexAndAssert($indexer_reindex, ["$base_dir/AA", "$base_dir/AB"], $database_contents, [
            "info: Updated 1 file(s).",
            "info: Reindexed 1 file(s) which were already indexed and equal.",
        ]);

        // Move a file around. This is the same as removing and adding a file;
        // FileIndexer doesn't have rename detection. (Also implicitly test:
        // the directory which is stored, should not be influenced by the value
        // passed to processPaths() / starting from a subdirectory. Though
        // that's obvious.)
        rename("$base_dir/aa/bb/cc/AA", "$base_dir/aa/bb/cc/Ax");
        $database_contents[] = ['aa/bb/cc', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        // File should be added as extra file; existing record is not removed
        // but a warning is logged (checkIndexedRecordsNonexistentInDir()).
        $this->indexAndAssert($indexer, ["$base_dir/aa/bb"], $database_contents, [
            "warning: Indexed records exist for the following nonexistent files in directory 'aa/bb/cc': AA.",
            "info: Added 1 new file(s).",
        ]);
        unset($database_contents[2]);
        // Remove 'AA' now.
        $this->indexAndAssert($indexer_remove, ["$base_dir/aa/bb"], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory 'aa/bb/cc': AA.",
            "info: Skipped 1 already indexed file(s).",
        ]);

        // Rename a file to a different case.
        rename("$base_dir/aa/bb/cc/Ax", "$base_dir/aa/bb/cc/AX");
        // (Unlike when we would have a case sensitive db) this is not the same
        // as removing and adding a file, because the mis-cased database row
        // just keeps referring to the renamed file / does not need an update.
        $this->indexAndAssert($indexer, ["$base_dir/aa/bb"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // Reindexing should update (re-case) the filename in the db.
        $database_contents[3][1] = 'AX';
        $this->indexAndAssert($indexer_reindex, ["$base_dir/aa/bb/cc"], $database_contents, [
            "info: Updated 1 file(s).",
        ]);

        // Rename a directory to a different case, see if reindexing it will
        // not do anything -> embedded in next step 0.

        // Test warning/removal of records in subdirectories that don't exist
        // anymore. (checkIndexedRecordsInNonexistentSubdirs().)
        // NOTE - this is one of two sub-tests which needs the 'subdirsCache'
        // property populated. So we're implicitly testing the SQL query which
        // populates that, but not when we're querying the complete $base_dir;
        // we do that in the checkIndexedRecordsInNonexistentDir() test.
        // 0: Check if a subdirsCache containing 'duplicate' entries with
        //    varying case, works OK: re-case two layers of subdirectories, to
        //    make files end up in the database with different directory case.
        rename("$base_dir/aa/bb/cc", "$base_dir/aa/bb/cC");
        rename("$base_dir/aa/bb", "$base_dir/aa/bB");
        rename("$base_dir/aa", "$base_dir/aA");
        // Now copy a new file and index it; we'll have bB/cC/AB and bb/cc/AX.
        copy("$base_dir/AB", "$base_dir/aA/bB/cC/AB");
        $database_contents[] = ['aA/bB/cC', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->indexAndAssert($indexer, ["$base_dir/aA/bB/cC/AB"], $database_contents, [
            "info: Added 1 new file(s).",
        ]);
        // ...and reindex: if subdirsCache works well, this won't log warnings.
        $this->indexAndAssert($indexer, ["$base_dir/aA"], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);
        // Just move the directory to another file.
        rename("$base_dir/aA/bB/cC", "$base_dir/cc");
        // 1a: See if the query still picks up the records which are now in a
        //     two layers deep missing directory. (There are warnings about
        //     'two' directories now because we just display every casing of
        //     a missing directory that is found in the database.)
        $this->indexAndAssert($indexer, ["$base_dir/aA"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory 'aA/bB': cC, cc.",
        ]);
        // 1b: Remove the now-empty bb; see if the query still picks up the
        //     records which are now in a two layers deep missing directory.
        rmdir("$base_dir/aA/bB");
        $this->indexAndAssert($indexer, ["$base_dir/aA"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory 'aA': bB, bb.",
        ]);
        // 2a: Test that the 2 records for aa/bb(/cc) are removed. (The cache
        //     has directories with different case but only one is logged,
        //     because the SQL query can't delete one of them at a time.)
        unset($database_contents[3]);
        unset($database_contents[4]);
        $this->indexAndAssert($indexer_remove, ["$base_dir/aA"], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'aA/bB'.",
         ]);
        // Recap: we only have two indexed files named AB and AX, and we have
        // two files in cc/ which are not indexed yet. For 2b, we first need to
        // index them.
        $database_contents[2] = ['cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $database_contents[3] = ['cc', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        $this->indexAndAssert($indexer_remove, ["$base_dir/cc"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);
        // 2b: Test again that the 2 records for cc are removed. The difference
        //     with 2a is that the SQL query operates on a directory that is
        //     directly in the root directory. (This difference is basically
        //     encoded by concatenateRelativePath(), and as long as that stays
        //     the same, we only need to do this test variation once, not in
        //     all tests which use the same LIKE construct for deletions.)
        //     Another difference: don't test the varying casing again.
        // Also, just rename aA back to aa so we don't need to change more code below. @todo check if we want to keep this.
        rename("$base_dir/aA", "$base_dir/aa");
        rename("$base_dir/cc", "$base_dir/aa/cc");
        // We can't just index 'cc' because we'll get a "not found". So reindex
        // the whole base directory. We won't reindex the entries in the  'aa'
        // directory because there's still a 'AA' file, so that's why we get
        // the below warning - but that's not what this test is about.
        unset($database_contents[2]);
        unset($database_contents[3]);
//        $database_contents[2][0] = $database_contents[3][0] = 'aa/cc';
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "warning: Directory '' contains entries for both AA and aa; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'cc'.",
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name. (checkIndexedRecordWithSameNameAsDir().)
        unlink("$base_dir/AA");
// Note that as a consequence of previous tests, directory 'aa' is still there
// so we don't need to place something on top of 'AA' - those are equivalent.
//        rename("$base_dir/aa/cc", "$base_dir/AA");
        // Reindex dir 'aa' now that 'AA' doesn't prevent it.
        $database_contents = [
            ['', 'AA', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty. Does not exist anymore .
            ['', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi
            ['aa/cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - this now gets reindexed.
            ['aa/cc', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ];
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            "warning: Indexed record exists for file 'AA', which actually matches a directory.",
            // 'aa' was not indexed yet; not connected with this specific test:
            "info: Added 2 new file(s).",
            "info: Skipped 1 already indexed file(s)."
        ]);
        // Remove the duplicate indexed item for nonexistent file.
        unset($database_contents[0]);
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed indexed record for file 'AA' which actually matches a directory.",
            "info: Skipped 3 already indexed file(s).",
        ]);

        // Re-setup directories with varying casing for the next test like we
        // did for the checkIndexedRecordsInNonexistentSubdirs() tests,
        // to maximize test surface. (We won't test 2 different ones for root &
        // subdir though; see previous comment at 2b.)
        rename("$base_dir/aa/cc", "$base_dir/aa/cC");
        rename("$base_dir/aa", "$base_dir/aA");
        copy("$base_dir/AB", "$base_dir/aA/x0");
        copy("$base_dir/AB", "$base_dir/aA/cC/x1");
        $database_contents[4] = ['aA', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        $database_contents[5] = ['aA/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        $this->indexAndAssert($indexer, ["$base_dir/aA/x0", "$base_dir/aA/cC/x1"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);
        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name. (checkIndexedRecordsInNonexistentDir().)
        // First move the directory out of the way; easier than removing,
        // though we'll have more extra reindexed records to deal with.
        rename("$base_dir/aA", "$base_dir/zz");
        // Move re-cased file in the place of the directory, for even more test.
        rename("$base_dir/AB", "$base_dir/AA");
        // Reinitialize again
        $database_contents = [
            ['', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - will now be reindexed.
            ['', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - moved to AA now.
            ['aA', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // these 4 records are moved now
            ['aa/cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aa/cc', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aA/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cC', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cC', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ];
        // This sub-test needs subdirsCache filled, so we'll only find these
        // records if we reindex the parent. NOTE - as mentioned above, this
        // test also needs to index / find / remove stuff from $base_dir,
        // because that properly tests the SQL query which fills subdirsCache.
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            // First warning is just a side effect; we're testing for the 2nd.
            "warning: Indexed records exist for the following nonexistent files in directory '': AB.",
            "warning: Indexed records exist with 'AA' (which is a file) as nonexistent base directory.",
            "info: Added 5 new file(s).",
        ]);
        // Remove the duplicate indexed items for nonexistent files.
        unset($database_contents[1]);
        unset($database_contents[2]);
        unset($database_contents[3]);
        unset($database_contents[4]);
        unset($database_contents[5]);
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory '': AB.",
            "info: Removed 4 indexed record(s) with 'AA' (which is a file) as nonexistent base directory.",
            "info: Skipped 5 already indexed file(s).",
        ]);

        // Remove the directory after the test.
        $processor = new PathRemover($logger);
        $processor->processPaths([$base_dir]);
    }

    /**
     * Test case insensitive file system with case sensitive database.
     */
    public function testIfileSdb()
    {
        // @todo the extra test. doTestIfile() does init and teardown, so we should change that while adding the test.
        $this->doTestIfile(false);
    }

    /**
     * Test case insensitive file system with case insensitive database.
     */
    public function testIfileIdb()
    {
        $this->doTestIfile(true);
    }

    private function doTestIfile($case_insensitive_database) {
        // Note below tests assume knowledge about the created file structure.
        $base_dir = $this->createCaseInsensitiveFileStructure();
        if (!$base_dir) {
            $this->markTestSkipped('Case sensitive directory is not configured or not actually case sensitive.');
            return;
        }

        $this->createDatabase($case_insensitive_database, true);

        $logger = new TestLogger();
        $indexer_default_config = [
            'pdo' => $this->pdo_connection,
            'allowed_base_directory' => $base_dir,
            'case_insensitive_filesystem' => true,
            'case_insensitive_database' => $case_insensitive_database,
        ];

        $indexer = new FileIndexer($logger, $indexer_default_config);
        $indexer_reindex = new FileIndexer($logger, $indexer_default_config + ['reindex_all' => true]);
        $indexer_remove = new FileIndexer($logger, $indexer_default_config + ['remove_nonexistent_from_index' => true]);

        $database_contents = [
            ['', 'AB', 'da39a3ee5e6b4b0d3255bfef95601890afd80709'], // empty
            ['aa/bb/cc', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi
        ];
        // For increased testing coverage, pass separate path names into the
        // method. Recursive processing will be tested alongside processing of
        // separate paths, because aa contains subdirs. AA and aa both will be
        // processed (even though that will cause strangeness further below)
        // because they are passed as separate arguments.
        $this->indexAndAssert($indexer, ["$base_dir/AB", "$base_dir/aa"], $database_contents, [
            "error: $base_dir/aa/BX is a symlink; this is not supported.",
            "info: Added 2 new file(s).",
            "info: Skipped 1 symlink(s).",
        ]);
        // End symlink tests.
        unlink("$base_dir/aa/BX");

        // Reindex the same directory, to see if anything changes now that the
        // database has contents. (This also tests if files in the 'root'
        // directory can be read back from the DB; conceivably, a DB system
        // could mess up empty string vs. null.) Also, see if we can refer to
        // a directory by a name with a different case, without side effects.
        $this->indexAndAssert($indexer, ["$base_dir/Aa/BB", "$base_dir/AB"], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);
        // Now reindex again while specifying the base dir.
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Reindexing will update the database with the provided path name, not
        // cased as on disk. (We don't necessarily want this but we've chosen
        // to not re-stat every input argument.) This will lead to an "updated"
        // log, also when all other data except the filename are the same.
        // Also, arguments will be automatically de-duplicated (if we
        // configured case insensitive fs).
        $database_contents[1][1] = 'Aa';
        $this->indexAndAssert($indexer_reindex, ["$base_dir/aa/bb/cc/Aa", "$base_dir/aa/bb/CC/AA"], $database_contents, [
            "info: Updated 1 file(s)."
        ]);
        // Same for directory. Oh, and deduplication isn't super smart.
        $database_contents[1][0] = 'aa/BB/cc';
        $database_contents[1][1] = 'AA';
        $this->indexAndAssert($indexer_reindex, ["$base_dir/aa/bb/CC/Aa", "$base_dir/aa/BB"], $database_contents, [
            "info: Updated 2 file(s).",
        ]);
        // Reindex 'back to actual dir name' so the next tests won't have an
        // "Updated" message where we don't expect it. Also verify message for
        // 'equal' files.
        $database_contents[1][0] = 'aa/bb/cc';
        $this->indexAndAssert($indexer_reindex, ["$base_dir"], $database_contents, [
            "info: Updated 1 file(s).",
            "info: Reindexed 1 file(s) which were already indexed and equal.",
        ]);

        // Actually rename a file to a different case.
        rename("$base_dir/aa/bb/cc/AA", "$base_dir/aa/bb/cc/Aa");
        // Maybe there are case insensitive file systems which don't actually
        // re-case the file. Even though the next test is still fine, we want
        // to know / the one after probably won't. So in this case just throw
        // an exception and we'll figure out what to do if that ever happens.
        $contents = scandir("$base_dir/aa/bb/cc");
        if (!in_array('Aa', $contents, true)) {
            throw new RuntimeException("Re-casing a file apparently does not actually change its filename. The tests should be inspected and probably changed to deal with that.");
        }
        if (in_array('AA', $contents, true)) {
            throw new RuntimeException("File was renamed (re-cased) but the old name is still there; impossible?");
        }
        // (Unlike when we would have a case sensitive db) this is not the same
        // as removing and adding a file, because the mis-cased database row
        // just keeps referring to the renamed file / does not need an update.
        $this->indexAndAssert($indexer, ["$base_dir/aa/bb"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // Reindexing should update (re-case) the filename in the db.
        $database_contents[1][1] = 'Aa';
        $this->indexAndAssert($indexer_reindex, ["$base_dir/aa/bb/cc"], $database_contents, [
            "info: Updated 1 file(s).",
        ]);

        // Change a file's contents and then reindex it. (There is no
        // difference here in whether it's a singular file or part of a
        // directory; all that logic is in processFile().)
        copy("$base_dir/aa/bb/cc/AA", "$base_dir/AB");
        // This will not change anything.
        $this->indexAndAssert($indexer, ["$base_dir/AB"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // We need to pass 'reindex_all' for this to have effect.
        $database_contents[0][2] = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'; // hi
        $this->indexAndAssert($indexer_reindex, ["$base_dir/AB"], $database_contents, [
            "info: Updated 1 file(s).",
        ]);

        // Move a file around. This is the same as removing and adding a file;
        // FileIndexer doesn't have rename detection. (Also implicitly test:
        // the directory which is stored, should not be influenced by the value
        // passed to processPaths() / starting from a subdirectory. Though
        // that's obvious.)
        rename("$base_dir/aa/bb/cc/Aa", "$base_dir/aa/bb/cc/Ax");
        $database_contents[] = ['aa/bb/cc', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        // File should be added as extra file; existing record is not removed
        // but a warning is logged (checkIndexedRecordsNonexistentInDir()).
        $this->indexAndAssert($indexer, ["$base_dir/aa/bb"], $database_contents, [
            "warning: Indexed records exist for the following nonexistent files in directory 'aa/bb/cc': Aa.",
            "info: Added 1 new file(s).",
        ]);
        unset($database_contents[1]);
        // Remove 'Aa' now.
        $this->indexAndAssert($indexer_remove, ["$base_dir/aa/bb"], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory 'aa/bb/cc': Aa.",
            "info: Skipped 1 already indexed file(s).",
        ]);


        // Test warning/removal of records in subdirectories that don't exist
        // anymore. (checkIndexedRecordsInNonexistentSubdirs().)
        // NOTE - this is one of two sub-tests which needs the 'subdirsCache'
        // property populated. So we're implicitly testing the SQL query which
        // populates that, but not when we're querying the complete $base_dir;
        // we do that in the checkIndexedRecordsInNonexistentDir() test.
        // 0: Check if a subdirsCache containing 'duplicate' entries with
        //    varying case, works OK: re-case two layers of subdirectories, to
        //    make files end up in the database with different directory case.
        // Now copy a new file and index it; we'll have bB/cC/AB and bb/cc/AX.
        copy("$base_dir/AB", "$base_dir/aa/bb/cc/AB");
        $database_contents[] = ['aA/bB/cC', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->indexAndAssert($indexer, ["$base_dir/aA/bB/cC/AB"], $database_contents, [
            "info: Added 1 new file(s).",
        ]);
        // ...and reindex: if subdirsCache works well, this won't log warnings.
        $this->indexAndAssert($indexer, ["$base_dir/aA"], $database_contents, [
            "info: Skipped 2 already indexed file(s).",
        ]);
        // Just move the directory to another file.
        rename("$base_dir/aa/bb/cc", "$base_dir/cc");
        // 1a: See if the query still picks up the records which are now in a
        //     two layers deep missing directory. (There are warnings about
        //     'two' directories now because we just display every casing of
        //     a missing directory that is found in the database.)
        $this->indexAndAssert($indexer, ["$base_dir/aA"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory 'aA/bb': cC, cc.",
        ]);
        // 1b: Remove the now-empty bb; see if the query still picks up the
        //     records which are now in a two layers deep missing directory.
        rmdir("$base_dir/aA/bB");
        $this->indexAndAssert($indexer, ["$base_dir/aA"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory 'aA': bB, bb.",
        ]);
        // 2a: Test that the 2 records for aa/bb(/cc) are removed. (The cache
        //     has directories with different case but only one is logged,
        //     because the SQL query can't delete one of them at a time.)
        unset($database_contents[2]);
        unset($database_contents[3]);
        $this->indexAndAssert($indexer_remove, ["$base_dir/aA"], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'aA/bB'.",
        ]);
        // Recap: we only have two indexed files named AB and AX, and we have
        // two files in cc/ which are not indexed yet. For 2b, we first need to
        // index them.
        $database_contents[2] = ['cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $database_contents[3] = ['cc', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        $this->indexAndAssert($indexer_remove, ["$base_dir/cc"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);
        // 2b: Test again that the 2 records for cc are removed. The difference
        //     with 2a is that the SQL query operates on a directory that is
        //     directly in the root directory. (This difference is basically
        //     encoded by concatenateRelativePath(), and as long as that stays
        //     the same, we only need to do this test variation once, not in
        //     all tests which use the same LIKE construct for deletions.)
        //     Another difference: don't test the varying casing again.
        // Also, just rename aA back to aa so we don't need to change more code below. @todo check if we want to keep this.
        rename("$base_dir/cc", "$base_dir/aa/cc");
        // We can't just index 'cc' because we'll get a "not found". So reindex
        // the whole base directory.
        $database_contents[2][0] = $database_contents[3][0] = 'aa/cc';
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'cc'.",
            "info: Added 2 new file(s).",
            "info: Skipped 1 already indexed file(s).",
        ]);

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name. (checkIndexedRecordWithSameNameAsDir().)
        rename("$base_dir/AB", "$base_dir/AX");
        rename("$base_dir/aa/cc", "$base_dir/AB");
        $database_contents = [
            ['', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // Does not exist anymore .
            ['', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // Gets reindexed.
            ['AB', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // Gets reindexed.
            ['AB', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aa/cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // Does not exist anymore.
            ['aa/cc', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ];
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            // Just a consequence of the renames; we're not testing this here.
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory 'aa': cc.",
            "warning: Indexed record exists for file 'AB', which actually matches a directory.",
            // This too.
            "info: Added 3 new file(s).",
        ]);
        // Remove the duplicate indexed item for nonexistent file.
        unset($database_contents[0]);
        unset($database_contents[4]);
        unset($database_contents[5]);
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'aa/cc'.",
            "info: Removed indexed record for file 'AB' which actually matches a directory.",
            "info: Skipped 3 already indexed file(s).",
        ]);

        // Re-setup directories with varying casing for the next test like we
        // did for the checkIndexedRecordsInNonexistentSubdirs() tests,
        // to maximize test surface. (We won't test 2 different ones for root &
        // subdir though; see previous comment at 2b.)
        copy("$base_dir/AX", "$base_dir/aA/x0");
        rename("$base_dir/AB", "$base_dir/aa/cc");
        copy("$base_dir/AX", "$base_dir/aA/cC/x1");
        $database_contents[4] = ['aA', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        $database_contents[5] = ['aA/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'];
        $this->indexAndAssert($indexer, ["$base_dir/aA/x0", "$base_dir/aA/cC/x1"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);
        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name. (checkIndexedRecordsInNonexistentDir().)
        // First move the directory out of the way; easier than removing,
        // though we'll have more extra reindexed records to deal with.
        rename("$base_dir/aA", "$base_dir/zz");
        // Move re-cased file in the place of the directory, for even more test.
        rename("$base_dir/AX", "$base_dir/AA");
        // Reinitialize again
        $database_contents = [
            ['', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - will now be reindexed.
            ['', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - moved to AA now.
            ['AB', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // these 4 records are moved now
            ['AB', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aA', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // hi - moved to AA now.
            ['aA/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cc', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cc', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ];
        // This sub-test needs subdirsCache filled, so we'll only find these
        // records if we reindex the parent. NOTE - as mentioned above, this
        // test also needs to index / find / remove stuff from $base_dir,
        // because that properly tests the SQL query which fills subdirsCache.
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            // First 2 warning are just side effects; we're testing for the 3rd.  <<< @TODO are both side effects? See if you can explain
            "warning: Indexed records exist for the following nonexistent files in directory '': AX.",
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory '': AB.",
            "warning: Indexed records exist with 'AA' (which is a file) as nonexistent base directory.",
            "info: Added 5 new file(s).",
        ]);
        // Remove the duplicate indexed items for nonexistent files.
        unset($database_contents[1]);
        unset($database_contents[2]);
        unset($database_contents[3]);
        unset($database_contents[4]);
        unset($database_contents[5]);
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory '': AX.",
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'AB'.",
            "info: Removed 2 indexed record(s) with 'AA' (which is a file) as nonexistent base directory.",
            "info: Skipped 5 already indexed file(s).",
        ]);

        // Remove the directory after the test.
        $processor = new PathRemover($logger);
        $processor->processPaths([$base_dir]);
    }

    //@todo the actual sensitive db + insensitive fs test: get 2 equivalent rows into the db somehow.



//@todo now check all todos in FileIndexer again.


//@todo get rid of all the throws at rename, unlink, etc

// @todo ...and then see if we can split out some code?

// @TODO set the 'LIKE pragma' where you shouldn't (and the other way around),
//  and see if you get errors.

//@TODO can we assert that recordsCache is empty, so we don't fill up memory?
//@TODO can/should we test that recordsCache gets added/removed from, during the time that files are processed?
//@todo we should maybe make some test class so we can peek into subdirsCache to see if it's expected?

//@todo cleanup files / dirs

//@todo try to process unreadable fiel, see what it does?

//@todo check what we're doing with symlinks that point to outside the directory - should give error.
//  ^ <-- maybe that should be a test not for FileIndexer but for FileRemover
//     ^ <- or no - that should just remove the symlink regardless of where it points. Test THAT!

//@todo tests for running with a file indexer with the wrong case sensitivity settings,
//  check if it at least gives 'normal' errors?
//  (Maybe I'm just throwing  "Database statement execution failed." in my code. I've seen
//  that when running the first part of FileSDbI with insensitive=false.)
//  ^<-- this should result in docs which say how important it is to get those configs right
//     (and, I guess, note that the 2 defaults are different.)

// @todo check that all PathRemovers have zero errors. And check manually that all the directories are clened out,
//   after a successful test.
}