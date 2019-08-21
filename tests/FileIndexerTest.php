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

        // Change a file's contents and then reindex it.
        $database_contents = $this->doTestReindexContents($base_dir, $indexer, $indexer_reindex, $database_contents, 'aa/bb/cc/AA', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42', 1);

        // Test warning/removal of records for files missing in a directory. We
        // 're-case' the file here for extra test surface. With a case
        // sensitive file system, this should 'move' the file - and combined
        // with a case sensitive database, this should be reflected in the db.
        // (Can't be done if either file system or database is case sensitive.)
        $database_contents = $this->doTestCheckIndexedRecordsNonexistentInDir($base_dir, $indexer, $indexer_remove, $database_contents, 'aa/bb/cc/AA', 'Aa', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42',
            // 'aa' gets found and skipped.
            'info: Skipped 1 already indexed file(s).'
        );

        // Test warning/removal of records in subdirectories that don't exist
        // anymore.
        $database_contents = $this->doTestCheckIndexedRecordsInNonexistentSubdirs($base_dir, $indexer, $indexer_remove, $database_contents, 'aa/bb/cc', 'cc', 'aa/cc');

        // To unify results in the next test, remove AB first.
        unlink("$base_dir/AB");
        unset($database_contents[1]);
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory '': AB.",
            "info: Skipped 3 already indexed file(s).",
        ]);

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name.
        $old_file_new_dir = 'AA';
        $old_dir = 'aa/cc';
        $oldfile_hash = 'da39a3ee5e6b4b0d3255bfef95601890afd80709';
        // We do test the directory structure a little bit extra, by having two
        // files in there with the same name and different case:
        $dirfile1_name = 'Aa';
        $dirfile1_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile2_name = 'aa';
        $dirfile2_hash = '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed';
        $this->doTestCheckIndexedRecordWithSameNameAsDir($base_dir, $indexer, $indexer_remove, $old_file_new_dir, $old_dir, $oldfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash);

        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name.
        $old_dir_new_file = 'AA';
        $new_moved_dir = 'cc';
        // $old_file doesn't even exist beforehand; gets copied after moving.
        $old_file = 'cc/Aa';
        $oldnewfile_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $this->doTestCheckIndexedRecordsInNonexistentDir($base_dir, $indexer, $indexer_remove, $old_file, $old_dir_new_file, $new_moved_dir, $oldnewfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash);

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
        // Remove the symlink; it will just cause noise in logs from now on.
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

        // In order to not complicate further tests:
        // - get rid of aa so we don't have to deal with warnings anymore. This
        //   ends "what happens with 2 files with different case" tests.
        unlink("$base_dir/aa/bb/cc/aa");
        // - unlink 'AA' so that directory 'aa' gets indexed and we have no
        //   more warnings;
        // - reindex-remove now, so we don't get the removal message at a later
        //   random time.
        unlink("$base_dir/AA");
        unset($database_contents[0]);
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed indexed record for file 'AA' which actually matches a directory.",
            "info: Skipped 2 already indexed file(s).",
        ]);

        // Change a file's contents and then reindex it.
        $database_contents = $this->doTestReindexContents($base_dir, $indexer, $indexer_reindex, $database_contents, 'aa/bb/cc/AA', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42', 1);

        // Test warning/removal of records for files missing in a directory.
        $database_contents = $this->doTestCheckIndexedRecordsNonexistentInDir($base_dir, $indexer, $indexer_remove, $database_contents, 'aa/bb/cc/AA', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42');

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

        // Test warning/removal of records in subdirectories that don't exist
        // anymore.
        // 0: Check if a subdirsCache containing 'duplicate' entries with
        //    varying case, works OK: re-case two layers of subdirectories, to
        //    make files end up in the database with different directory case.
        rename("$base_dir/aa/bb/cc", "$base_dir/aa/bb/cC");
        rename("$base_dir/aa/bb", "$base_dir/aa/bB");
        rename("$base_dir/aa", "$base_dir/aA");
        // Now copy a new file and index it; we'll have bB/cC/AB and bb/cc/AX.
        copy("$base_dir/AB", "$base_dir/aA/bB/cC/AB");
        $database_contents[] = ['aA/bB/cC', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $database_contents = $this->doTestCheckIndexedRecordsInNonexistentSubdirs($base_dir, $indexer, $indexer_remove, $database_contents, 'aA/bB/cC', 'cc', 'aA/cc', 'aA/bB/cC/AB', 'cc', 'bb');

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name. To give the test an extra edge, we'll make the
        // file & directory have the same name except for case. Use 'AA' & 'aA'.
        rename("$base_dir/AB", "$base_dir/AA");
        // Set database up for the test / get messages out of the way.
        $database_contents = [
            ['', 'AA', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ];
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "warning: Directory '' contains entries for both AA and aA; these cannot both be indexed in a case insensitive database. Skipping the latter file.",
            "info: Removed 1 indexed record(s) for nonexistent files in directory '': AB.",
            "info: Removed 2 indexed record(s) with 'AA' (which is a file) as nonexistent base directory.",
            "info: Added 1 new file(s).",
        ]);
        $old_file_new_dir = 'AA';
        $old_dir = 'aA/cc';
        $oldfile_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile1_name = 'AB';
        $dirfile1_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile2_name = 'AX';
        $dirfile2_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $this->doTestCheckIndexedRecordWithSameNameAsDir($base_dir, $indexer, $indexer_remove, $old_file_new_dir, $old_dir, $oldfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash);

        // Re-setup directories with varying casing for the next test like we
        // did for doTestCheckIndexedRecordsInNonexistentSubdirs() to
        // maximize test surface.
        rename("$base_dir/aA/cc", "$base_dir/aA/cC");
        rename("$base_dir/aA", "$base_dir/aa");
        // Move 2 files and reindex them; keep records with old cases too.
        rename("$base_dir/aa/cC/AB", "$base_dir/aa/x0");
        rename("$base_dir/aa/cC/AX", "$base_dir/aa/cC/x1");
        $database_contents = [
            ['aa', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // these 4 records are moved now
            ['aA/cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aA/cc', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aa/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ];
        $this->indexAndAssert($indexer, ["$base_dir/aa/x0", "$base_dir/aa/cC/x1"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);

        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name.
        $old_dir_new_file = 'aa';
        $new_moved_dir = 'zz';
        // $old_file doesn't even exist beforehand; gets copied after moving.
        $old_file = 'zz/cC/x1';
        $oldnewfile_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $this->doTestCheckIndexedRecordsInNonexistentDir($base_dir, $indexer, $indexer_remove, $old_file, $old_dir_new_file, $new_moved_dir, $oldnewfile_hash, '', '', '', '', [
            // Doesn't exist anymore
            ['aA/cc', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['aA/cc', 'AX', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            // Will get moved / will have been moved. Note different cases.
            ['aa', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'], // these 4 records are moved now
            ['aa/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz', 'x0', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cC', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
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
        // Remove the symlink; it will just cause noise in logs from now on.
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

        // Change a file's contents and then reindex it.
        $database_contents = $this->doTestReindexContents($base_dir, $indexer, $indexer_reindex, $database_contents, 'aa/bb/cc/AA', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42', 0, false);

        // Test warning/removal of records for files missing in a directory.
        $database_contents = $this->doTestCheckIndexedRecordsNonexistentInDir($base_dir, $indexer, $indexer_remove, $database_contents, 'aa/bb/cc/Aa', 'Ax', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42');

        // Test warning/removal of records in subdirectories that don't exist
        // anymore.
        // Copy a new file and index it (inside the method); we'll have
        // bB/cC/AB and bb/cc/AX for extra test of point 0.
        copy("$base_dir/AB", "$base_dir/aa/bb/cc/AB");
        $database_contents[] = ['aA/bB/cC', 'AB', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42']; // hi
        $this->doTestCheckIndexedRecordsInNonexistentSubdirs($base_dir, $indexer, $indexer_remove, $database_contents, 'aA/bb/cc', 'cc', 'aa/cc','aA/bB/cC/AB', 'cC', 'bB');

        // Test warning/removal of an entry for a file that is now a directory
        // with the same name.
        $old_file_new_dir = 'AB';
        $old_dir = 'aa/cc';
        $oldfile_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile1_name = 'AB';
        $dirfile1_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $dirfile2_name = 'Ax';
        $dirfile2_hash = 'c22b5f9178342609428d6f51b2c5af4c0bde6a42';
        $this->doTestCheckIndexedRecordWithSameNameAsDir($base_dir, $indexer, $indexer_remove, $old_file_new_dir, $old_dir, $oldfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash);

        // Re-setup directories with varying casing for the next test like we
        // did for doTestCheckIndexedRecordsInNonexistentSubdirs(), to
        // maximize test surface.
        rename("$base_dir/AB", "$base_dir/aa/cc");
        rename("$base_dir/aa/cc/Ax", "$base_dir/aa/x0");
        $database_contents = [
            ['aa', 'x0', $oldfile_hash],
            ['aa/cc', 'AB', $oldfile_hash],
        ];
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory 'AB'.",
            "info: Added 2 new file(s).",
        ]);
        rename("$base_dir/aa/x0", "$base_dir/aa/x00");
        rename("$base_dir/aa/cc/AB", "$base_dir/aa/cc/x1");
        $database_contents[] = ['aA', 'X00', $oldfile_hash];
        $database_contents[] = ['aA/cC', 'x1', $oldfile_hash];
        $this->indexAndAssert($indexer, ["$base_dir/aA/X00", "$base_dir/aA/cC/x1"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);

        // Test warning/removal of records within a (sub)directory which is now
        // a file with the same name.
        $old_dir_new_file = 'aa';
        $new_moved_dir = 'zz';
        // $old_file doesn't even exist beforehand; gets copied after moving.
        $old_file = 'zz/cc/x1';
        $this->doTestCheckIndexedRecordsInNonexistentDir($base_dir, $indexer, $indexer_remove, $old_file, $old_dir_new_file, $new_moved_dir, $oldfile_hash, '', '', '', '', [
            // Doesn't exist anymore
            ['aa', 'x0', $oldfile_hash],
            ['aa/cc', 'AB', $oldfile_hash],
            // Will get moved / will have been moved. Note different cases.
            ['aA', 'X00', $oldfile_hash],
            ['aA/cC', 'x1', $oldfile_hash],
            ['zz', 'x00', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
            ['zz/cc', 'x1', 'c22b5f9178342609428d6f51b2c5af4c0bde6a42'],
        ]);

        // Remove the directory after the test.
        $processor = new PathRemover($logger);
        $processor->processPaths([$base_dir]);
    }

    /**
     * Helper to test reindexing contents and other things.
     *
     * @param string $base_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_reindex
     * @param array[] $database_contents
     * @param $copy_from
     * @param $copy_to
     * @param $new_hash
     * @param int $index_in_contents
     *   Index of the file we're changing in the database contents. We can
     *   derive this instead of havind a parameter, but I'm lazy / it just
     *   creates more code, and code readability is already questionable.
     * @param bool $test_reindex_message
     *   (Optional) if true / by default, do extra unrelated test.
     *
     * @return array[]
     *   The modified database contents.
     */
    function doTestReindexContents($base_dir, $indexer, $indexer_reindex, $database_contents, $copy_from, $copy_to, $new_hash, $index_in_contents, $test_reindex_message = true)
    {
        // Change a file's contents and then reindex it - by copying. (There is
        // no difference here in whether it's a singular file or part of a
        // directory; all the tested logic is in processFile().)
        copy("$base_dir/$copy_from", "$base_dir/$copy_to");
        // This will not change anything.
        $this->indexAndAssert($indexer, ["$base_dir/$copy_to"], $database_contents, [
            "info: Skipped 1 already indexed file(s).",
        ]);
        // We need to pass 'reindex_all' for this to have effect. Also verify
        // message for 'equal' files.
        $database_contents[$index_in_contents][2] = $new_hash;
        $reindex_files = ["$base_dir/$copy_to"];
        $logs = ["info: Updated 1 file(s)."];
        if ($test_reindex_message) {
            $reindex_files[] = "$base_dir/$copy_from";
            $logs[] = 'info: Reindexed 1 file(s) which were already indexed and equal.';
        }
        $this->indexAndAssert($indexer_reindex, $reindex_files, $database_contents, $logs);

        return $database_contents;
    }

    /**
     * Helper to test checkIndexedRecordsNonexistentInDir().
     *
     * That is: warning/removal of records for files missing in a directory.
     *
     * @param string $base_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_remove
     * @param array[] $database_contents
     * @param string $old_file
     *   File to move; path relative to basedir.
     * @param string $moved_file
     *   Filename to move to (in the same directory); only filename.
     * @param string $file_hash
     * @param string $extra_log
     *
     * @return array[]
     *   The modified database contents.
     */
    private function doTestCheckIndexedRecordsNonexistentInDir($base_dir, $indexer, $indexer_remove, $database_contents, $old_file, $moved_file, $file_hash, $extra_log = '')
    {
        // For some assumptions below, it's important that the file is moved
        // within the same directory.
        $test_dir = dirname($old_file);
        $old_file = basename($old_file);

        // Move a file around. This is the same as removing and adding a file;
        // FileIndexer doesn't have rename detection.
        rename("$base_dir/$test_dir/$old_file", "$base_dir/$test_dir/$moved_file");
        $database_contents[] = [$test_dir, $moved_file, $file_hash];
        // Also implicitly test: the directory which is stored, should not be
        // influenced by the value passed to processPaths() / starting from a
        // subdirectory. That's obvious, but still: we're starting from the
        // parent directory for the heck of it.
        $reindex_dir = dirname($test_dir);
        // File should be added as extra file; existing record is not removed
        // but a warning is logged (checkIndexedRecordsNonexistentInDir()).
        $logs = [
            "warning: Indexed records exist for the following nonexistent files in directory '$test_dir': $old_file.",
            "info: Added 1 new file(s).",
        ];
        if ($extra_log) {
            $logs[] = $extra_log;
        }
        $this->indexAndAssert($indexer, ["$base_dir/$reindex_dir"], $database_contents, $logs);

        // Now remove the record.
        $database_contents = array_filter($database_contents, function ($element) use ($test_dir, $old_file, $file_hash) {
            return $element !== [$test_dir, $old_file, $file_hash];
        });
        $skip = array_filter($database_contents, function ($record) use ($reindex_dir) {
           return $record[0] === $reindex_dir || strpos($record[0], "$reindex_dir/") === 0;
        });
        $skipped = count($skip);
        $this->indexAndAssert($indexer_remove, ["$base_dir/$reindex_dir"], $database_contents, [
            "info: Removed 1 indexed record(s) for nonexistent files in directory '$test_dir': $old_file.",
            "info: Skipped $skipped already indexed file(s).",
        ]);

        return $database_contents;
    }

    /**
     * Helper to test checkIndexedRecordsInNonexistentSubdirs().
     *
     * That is: warning/removal of records in subdirectories that don't exist
     * anymore. Called from several tests, hence all the pesky abstraction.
     *
     * NOTE - this is one of two sub-tests which needs the 'subdirsCache'
     * property populated. So we're implicitly testing the SQL query which
     * populates that, but not when we're querying the complete $base_dir; we
     * do that in doTestCheckIndexedRecordsInNonexistentDir().
     *
     * @param string $base_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_remove
     * @param array[] $database_contents
     * @param string $old_subdir
     *   Subdir to be moved out of the way so we can see the errors. Also, the
     *   base of this directory is what gets reindexed. NOTE on case sensitive
     *   file systems you may re-case the base directory (which does not
     *   influence the rename command) if you want the files to be reindexed
     *   with a different case.
     * @param string $moved_dir
     * @param string $moved_dir_2b
     *   (Optional) directory to move the moved directory to again a second
     *   time. If empty, the caller must do the test themselves.
     * @param string $first_index_file
     *   (Optional) If nonempty, before starting, first index a file (that's
     *   already copied into place). See reasons below.
     * @param string $old_subdir_differently_indexed
     *   (Optional) Another case with which this directory may be indexed
     *   already, than is given in $old_subdir. This will then be shown in
     *   warning messages.
     * @param string $old_basedir_differently_indexed
     *   (Optional) Same for the directory just below.
     *
     * @return array[]
     *   The modified database contents.
     */
    private function doTestCheckIndexedRecordsInNonexistentSubdirs($base_dir, $indexer, $indexer_remove, $database_contents, $old_subdir, $moved_dir, $moved_dir_2b, $first_index_file = '', $old_subdir_differently_indexed = '', $old_basedir_differently_indexed = '')
    {
        // $old_subdir can have 3 levels, in which case the 'base' and the
        // 'very base' ($reindex_base) are different.
        $old_sub_base = dirname($old_subdir);
        $reindex_base = strstr($old_subdir, '/', true);
        $nonexistent_subdirs = basename($old_subdir);
        // $nonexistent_subdirs is usually like 'cc') but can also be 'cC, cc'.
        if ($old_subdir_differently_indexed) {
            $dirs = [$nonexistent_subdirs, $old_subdir_differently_indexed];
            sort($dirs);
            $nonexistent_subdirs = implode(', ', $dirs);
        }

        if ($first_index_file) {
            // 0: Check if a subdirsCache containing 'duplicate' entries with
            //    varying case, works OK: re-case two layers of subdirectories,
            //    to make files end up in the database with different directory
            //    case. This is a continuation from the caller; copying and
            //    preparing $database_contents is already done.
            $this->indexAndAssert($indexer, ["$base_dir/$first_index_file"], $database_contents, [
                "info: Added 1 new file(s).",
            ]);
            // ...and reindex: if subdirsCache works well, this won't log warnings.
            $this->indexAndAssert($indexer, ["$base_dir/$reindex_base"], $database_contents, [
                "info: Skipped 2 already indexed file(s).",
            ]);
        }

        // Just move the directory elsewhere rather than removing it.
        rename("$base_dir/$old_subdir", "$base_dir/$moved_dir");
        // 1a: See if the query still picks up the records which are now in a
        //     two layers deep missing directory. (There are warnings about
        //     'two' directories now because we just display every casing of
        //     a missing directory that is found in the database.)
        $this->indexAndAssert($indexer, ["$base_dir/$reindex_base"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory '$old_sub_base': $nonexistent_subdirs.",
        ]);

        // 1b: Assumption: $old_sub_base is now empty, and is a multilayer
        //     directory whose first level we can remove. Remove it; see if the
        //     query still picks up the records which are now in a two layers
        //     deep missing directory.
        $nonexistent_subdirs = basename($old_sub_base);
        // $nonexistent_subdirs is usually like 'cc') but can also be 'cC, cc'.
        if ($old_basedir_differently_indexed) {
            $dirs = [$nonexistent_subdirs, $old_basedir_differently_indexed];
            sort($dirs);
            $nonexistent_subdirs = implode(', ', $dirs);
        }
        rmdir("$base_dir/$old_sub_base");
        $this->indexAndAssert($indexer, ["$base_dir/$reindex_base"], $database_contents, [
            "warning: Indexed records exist for files in the following nonexistent subdirectories of directory '$reindex_base': $nonexistent_subdirs.",
        ]);

        // 2a: Test that the 2 records for aa/bb(/cc) are removed. (Even if the
        //     cache has directories with different case but only one is logged,
        //     because the SQL query can't delete one of them at a time.)
        $moved_entry2 = array_pop($database_contents);
        $moved_entry1 = array_pop($database_contents);
        // Assume it's the "smallest" directory name that gets displayed.
        if ($old_basedir_differently_indexed && $old_basedir_differently_indexed < basename($old_sub_base)) {
            $old_sub_base = substr($old_sub_base, 0, strlen($old_sub_base) - strlen($old_basedir_differently_indexed)) . $old_basedir_differently_indexed;
        }
        $this->indexAndAssert($indexer_remove, ["$base_dir/$reindex_base"], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory '$old_sub_base'.",
        ]);
        // For 2b, we first need to index the files we've moved.
        $moved_entry1[0] = $moved_entry2[0] = $moved_dir;
        array_push($database_contents, $moved_entry1);
        array_push($database_contents, $moved_entry2);
        $this->indexAndAssert($indexer_remove, ["$base_dir/$moved_dir"], $database_contents, [
            "info: Added 2 new file(s).",
        ]);

        // 2b: Test again that the 2 records for $moved_dir are removed.
        //     The difference with 2a is that the SQL query operates on a
        //     directory that is directly in the root directory. (This
        //     difference is basically encoded by concatenateRelativePath(),
        //     and as long as that stays the same, we only need to do this
        //     test variation once, not in all tests which use the same
        //     LIKE construct for deletions.)
        rename("$base_dir/$moved_dir", "$base_dir/$moved_dir_2b");
        $moved_entry2 = array_pop($database_contents);
        $moved_entry1 = array_pop($database_contents);
        $moved_entry1[0] = $moved_entry2[0] = $moved_dir_2b;
        array_push($database_contents, $moved_entry1);
        array_push($database_contents, $moved_entry2);
        // We can't just index '$moved_dir' because we'll get a "not found". So
        // reindex the whole base directory.
        $count = count($database_contents) - 2;
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory '$moved_dir'.",
            "info: Added 2 new file(s).",
            "info: Skipped $count already indexed file(s).",
        ]);

        return $database_contents;
    }

    /**
     * Helper to test checkIndexedRecordsInNonexistentDir().
     *
     * That is: warning/removal of records within a (sub)directory which is now
     * a file with the same name. Called from several tests, hence all the
     * pesky abstraction.
     *
     * @param string $base_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_remove
     * @param string $old_file
     * @param string $old_dir_new_file
     * @param string $new_moved_dir
     * @param string $oldnewfile_hash
     * @param string $dirfile1_name
     * @param string $dirfile1_hash
     * @param string $dirfile2_name
     * @param string $dirfile2_hash
     * @param array $extra_indexed_records
     *   Often used for records of files that are indexed (because they might
     *   give some extra weight to the tests) but don't exist anymore (so they
     *   don't get reindexed at $new_moved_dir).
     *
     */
    private function doTestCheckIndexedRecordsInNonexistentDir($base_dir, $indexer, $indexer_remove, $old_file, $old_dir_new_file, $new_moved_dir, $oldnewfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash, array $extra_indexed_records = [])
    {
        // First move the directory out of the way; easier than removing.
        rename("$base_dir/$old_dir_new_file", "$base_dir/$new_moved_dir");
        // $old_file might not exist beforehand; can also be part of
        // $new_moved_dir. Also, if we can have only one case indexed in the
        // database, we'll change case of the new file for extra test surface.
        $case_insensitive_db = !empty($indexer->getConfig('case_insensitive_database'));
        $case_insensitive_fs = !empty($indexer->getConfig('case_insensitive_filesystem'));
        $new_file = $old_dir_new_file;
        if ($case_insensitive_fs || $case_insensitive_db) {
            $new_file = strtoupper($new_file);
            if ($new_file === $old_dir_new_file) {
                // We want to test this, so $old_dir_new_file needs tweaking.
                throw new RuntimeException("doTestCheckIndexedRecordWithSameNameAsDir conditions for sensitive fs / insensitive db seem to have changed. Please review.");
            }
        }
        copy("$base_dir/$old_file", "$base_dir/$new_file");

        // Compile database contents. This is complicated by the fact that some
        // files in the old directory are mis-cased (on purpose, to give tests
        // extra weight), so we can't just take $old_dir_new_file as directory.
        // Also, sometimes $old_dir_new_file / $new_moved_dir is multi-level.
        if ($dirfile1_name) {
            $database_contents = [
                // Moved here, so reindexed now, though that isn't tied to the test:
                [$new_moved_dir, $dirfile1_name, $dirfile1_hash],
                [$new_moved_dir, $dirfile2_name, $dirfile2_hash],
                // Moved away; tested here:
                [$old_dir_new_file, $dirfile1_name, $dirfile1_hash],
                [$old_dir_new_file, $dirfile2_name, $dirfile2_hash],
            ];
        } else {
            // Caller has to specify everything.
            $database_contents = $extra_indexed_records;
        }
        $tmp_dir = dirname($new_file);
        if ($tmp_dir === '.') {
            $tmp_dir = '';
        }
        // File moved into the old directory position; reindexed:
        $database_contents[] = [$tmp_dir, basename($new_file), $oldnewfile_hash];

        // This sub-test needs subdirsCache filled, so we'll only find these
        // records if we reindex the parent. NOTE - as mentioned at the
        // checkIndexedRecordsInNonexistentDir() tests, we need to pass
        // [$base_dir] as an argument because that properly tests the SQL query
        // which fills subdirsCache. That isn't done elsewhere yet.
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, [
            "warning: Indexed records exist with '$new_file' (which is a file) as nonexistent base directory.",
            "info: Added 3 new file(s).",
        ]);
        // Remove the duplicate indexed items for nonexistent files.
        if ($dirfile1_name) {
            $database_contents = [
                [$new_moved_dir, $dirfile1_name, $dirfile1_hash],
                [$new_moved_dir, $dirfile2_name, $dirfile2_hash],
            ];
        } else {
            $database_contents = array_filter($extra_indexed_records, function ($record) use ($new_moved_dir) {
               return $record[0] === $new_moved_dir || strpos($record[0], "$new_moved_dir/") === 0;
            });
        }
        $database_contents[] = [$tmp_dir, basename($new_file), $oldnewfile_hash];
        $count = $extra_indexed_records ? count($extra_indexed_records) - 2 : 2;
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, [
            "info: Removed $count indexed record(s) with '$new_file' (which is a file) as nonexistent base directory.",
            "info: Skipped 3 already indexed file(s).",
        ]);
    }

    /**
     * Helper to test checkIndexedRecordWithSameNameAsDir().
     *
     * That is: warning/removal of an entry for a file that is now a directory
     * with the same name. Called from several tests, hence all the pesky
     * abstraction.
     *
     * @param string $base_dir
     * @param FileIndexer $indexer
     * @param FileIndexer $indexer_remove
     * @param string $old_file_new_dir
     * @param string $old_dir
     * @param string $oldfile_hash
     * @param string $dirfile1_name
     * @param string $dirfile1_hash
     * @param string $dirfile2_name
     * @param string $dirfile2_hash
     */
    private function doTestCheckIndexedRecordWithSameNameAsDir($base_dir, $indexer, $indexer_remove, $old_file_new_dir, $old_dir, $oldfile_hash, $dirfile1_name, $dirfile1_hash, $dirfile2_name, $dirfile2_hash)
    {
        // Prep: move the file into the directory's place.
        $old_file = $new_dir = $old_file_new_dir;
        unlink("$base_dir/$old_file");
        $rename = true;
        $case_insensitive_db = !empty($indexer->getConfig('case_insensitive_database'));
        $case_insensitive_fs = !empty($indexer->getConfig('case_insensitive_filesystem'));
        if (!$case_insensitive_fs && $case_insensitive_db) {
            // If the base dirname of the file is the same os the old directory
            // (base) except for case, then one of them is not indexed. We
            // assume the old directory is not indexed (because otherwise we
            // wouldn't be able to test what we want to test here), so that
            // influences our test results below.
            $tmp_file = strtolower("$old_file_new_dir/");
            $tmp_dir = strtolower("$old_dir/");
            if (strpos($tmp_file, $tmp_dir) === 0 || strpos($tmp_dir, $tmp_file) === 0) {
                // Also, we will not move (re-case) the old directory, to give
                // some extra edge to our test on sensitive fs / insensitive db.
                $rename = false;
                $new_dir = $old_dir;
            } else {
                // We want to test the above.
                throw new RuntimeException("doTestCheckIndexedRecordWithSameNameAsDir conditions for sensitive fs and insensitive db seem to have changed. Please review.");
            }
        }
        if ($rename) {
            rename("$base_dir/$old_dir", "$base_dir/$new_dir");
        }

        $database_contents = [
            ['', $old_file, $oldfile_hash], // empty. Does not exist anymore.
            [$new_dir, $dirfile1_name, $dirfile1_hash], // This + next will now be reindexed.
            [$new_dir, $dirfile2_name, $dirfile2_hash],
        ];
        if ($new_dir !== $old_dir) {
            $database_contents[] = [$old_dir, $dirfile1_name, $dirfile1_hash]; // This + next does not exist anymore.
            $database_contents[] = [$old_dir, $dirfile2_name, $dirfile2_hash];
        }
        $logs = [
            "warning: Indexed record exists for file '$old_file_new_dir', which actually matches a directory.",
            "info: Added 2 new file(s).",
        ];
        if ($rename) {
            // This one is there because of previous state, we just put up with it.
            // The location unfortunately depends on $old_file_new_dir vs 'aa'.
            $tmp_dir = dirname($old_dir);
            $tmp_file = basename($old_dir);
            $unimportant_log = "warning: Indexed records exist for files in the following nonexistent subdirectories of directory '$tmp_dir': $tmp_file.";
            array_splice($logs, $old_file_new_dir === 'AA' ? 1 : 0, 0, [$unimportant_log]);
        }
        $this->indexAndAssert($indexer, [$base_dir], $database_contents, $logs);

        // Remove the duplicate indexed item for nonexistent file.
        unset($database_contents[0]);
        unset($database_contents[3]);
        unset($database_contents[4]);
        $logs = [
            "info: Removed indexed record for file '$old_file_new_dir' which actually matches a directory.",
            "info: Skipped 2 already indexed file(s).",
        ];
        if ($rename) {
            $unimportant_log = "info: Removed 2 indexed record(s) for file(s) in (subdirectories of) nonexistent directory '$old_dir'.";
            array_splice($logs, $old_file_new_dir === 'AA' ? 1 : 0, 0, [$unimportant_log]);
        }
        $this->indexAndAssert($indexer_remove, [$base_dir], $database_contents, $logs);
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
