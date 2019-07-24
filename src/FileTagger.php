<?php

namespace DataTest; 

/**
 * PathProcessor that tags files.
 */
class FileTagger extends PathProcessor {

  /**
   * Gets the machine name of the vocabulary containing the tags.
   *
   * This may need to be changed later to e.g. be configurable; a static method
   * returning a constant seemed best for now.
   */
  public static function getVocabularyMachineName() {
    return 'datatest';
  }

  /**
   * FileIndexer constructor.
   *
   * @param \Psr\Log\LoggerInterface $logger
   *   A logger instance.
   * @param array $config
   *   (Optional) configuration values.
   */
  public function __construct(\Psr\Log\LoggerInterface $logger, array $config = []) {
    parent::__construct($logger, $config);
    $this->config += [
      // Mysql is case sensitive
      'case_sensitive_database' => TRUE,
      'case_sensitive_filesystem' => FALSE,
      'reindex_all' => FALSE,
    ];
    $this->state += [
      'new' => 0,
      'updated' => 0,
      'equal' => 0,
      'skipped' => 0,
    ];
  }

  /**
   * {@inheritdoc}
   *
   * Also prints stats.
   *
   * @param string[] $paths
   *   Paths; can be relative or absolute; can be files or directories.
   */
  public function processPaths(array $paths) {
    parent::processPaths($paths);
    //@todo stats
  }

}
