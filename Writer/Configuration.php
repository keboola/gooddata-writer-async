<?php
/**
 * Configuration Wrapper
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\SharedStorageException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\StorageApi\CachedClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Syrup\Exception\UserException;

class Configuration
{
    const WRITER_NAME = 'gooddata';

    const PROJECTS_TABLE_NAME = 'projects';
    const USERS_TABLE_NAME = 'users';
    const PROJECT_USERS_TABLE_NAME = 'project_users';
    const FILTERS_TABLE_NAME = 'filters';
    const FILTERS_USERS_TABLE_NAME = 'filters_users';
    const FILTERS_PROJECTS_TABLE_NAME = 'filters_projects';
    const DATE_DIMENSIONS_TABLE_NAME = 'date_dimensions';
    const DATA_SETS_TABLE_NAME = 'data_sets';

    protected $cache = [];

    /**
     * Definition serves for automatic configuration of Storage API tables
     * @var array
     */
    public $tables = [
        self::PROJECTS_TABLE_NAME => [
            'columns' => ['pid', 'active'],
            'primaryKey' => 'pid',
            'indices' => []
        ],
        self::USERS_TABLE_NAME => [
            'columns' => ['email', 'uid'],
            'primaryKey' => 'email',
            'indices' => []
        ],
        self::PROJECT_USERS_TABLE_NAME => [
            'columns' => ['id', 'pid', 'email', 'role', 'action'],
            'primaryKey' => 'id',
            'indices' => ['pid', 'email']
        ],
        self::FILTERS_TABLE_NAME => [
            'columns' => ['name', 'attribute', 'operator', 'value', 'over', 'to'],
            'primaryKey' => 'name',
            'indices' => []
        ],
        self::FILTERS_USERS_TABLE_NAME => [
            'columns' => ['id', 'filter', 'email'],
            'primaryKey' => 'id',
            'indices' => ['filter', 'email']
        ],
        self::FILTERS_PROJECTS_TABLE_NAME => [
            'columns' => ['uri', 'filter', 'pid'],
            'primaryKey' => 'uri',
            'indices' => ['filter', 'pid']
        ],
        self::DATE_DIMENSIONS_TABLE_NAME => [
            'columns' => ['name', 'includeTime', 'template', 'isExported'],
            'primaryKey' => 'name',
            'indices' => []
        ],
        self::DATA_SETS_TABLE_NAME => [
            'columns' => ['id', 'name', 'export', 'isExported', 'lastChangeDate', 'incrementalLoad', 'ignoreFilter', 'definition'],
            'primaryKey' => 'id',
            'indices' => []
        ]
    ];

    public static $columnDefinitions = ['gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
        'format', 'dateDimension', 'sortLabel', 'sortOrder'];


    /** @var CachedClient */
    private $cachedClient;
    /** @var SharedStorage */
    private $sharedStorage;

    public $bucketId;
    public $projectId;
    public $writerId;
    public $tokenInfo;

    public $gdDomain = false;
    public $testingWriter = false;
    public $noDateFacts = false;


    /**
     * Prepare configuration
     * Get bucket attributes for Rest API calls
     */
    public function __construct(CachedClient $client, SharedStorage $sharedStorage)
    {
        $this->cachedClient = $client;
        $this->sharedStorage = $sharedStorage;

        $logData = $this->cachedClient->getClient()->getLogData();
        $this->projectId = $logData['owner']['id'];
        if (!empty($logData['owner']['features'])) {
            if (in_array('gdwr-academy', $logData['owner']['features'])) {
                $this->gdDomain = 'keboola-academy';
            }
            $this->testingWriter = in_array('gdwr-testing', $logData['owner']['features']);
        }
    }

    public function setWriterId($writerId)
    {
        $this->writerId = $writerId;
        $this->tokenInfo = $this->cachedClient->getClient()->getLogData();
        $this->projectId = $this->tokenInfo['owner']['id'];

        try {
            $writer = $this->sharedStorage->getWriter($this->projectId, $writerId);
            $this->bucketId = $writer['bucket'];
            $this->noDateFacts = !$writer['feats']['date_facts'];
        } catch (SharedStorageException $e) {
            $this->bucketId = 'sys.c-wr-gooddata-' . $writerId;
            try {
                $this->sharedStorage->createWriter($this->projectId, $writerId, $this->bucketId, $this->tokenInfo['id'], $this->tokenInfo['description']);
                $this->sharedStorage->setWriterStatus($this->projectId, $writerId, SharedStorage::WRITER_STATUS_READY);
            } catch (SharedStorageException $e) {
            }
        }
    }

    public function clearCache()
    {
        $this->cache = [];
        $this->cachedClient->clearCache();
    }


    /********************
     ********************
     * @section Writer and it's bucket
     ********************/

    /**
     * Create configuration bucket
     */
    public function createBucket($writerId)
    {
        $this->cachedClient->getClient()->createBucket('wr-gooddata-' . $writerId, 'sys', 'GoodData Writer Configuration');
        $this->cachedClient->getClient()->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'writer', self::WRITER_NAME);
        $this->cachedClient->getClient()->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'writerId', $writerId);
        $this->bucketId = 'sys.c-wr-gooddata-' . $writerId;
    }

    private static function parseBucketAttributes($attributes)
    {
        $result = [];
        foreach ($attributes as $attr) {
            $attrArray = explode('.', $attr['name']);
            if (count($attrArray) > 1) {
                if (!isset($result[$attrArray[0]])) {
                    $result[$attrArray[0]] = [];
                }
                $result[$attrArray[0]][$attrArray[1]] = $attr['value'];
            } else {
                $result[$attr['name']] = $attr['value'];
            }
        }

        $error = false;
        if (empty($result['gd'])) {
            $error = 'The writer is missing GoodData project configuration. You cannot perform any GoodData operations. See the docs please.';
        } elseif (empty($result['gd']['pid'])) {
            $error = 'The writer is missing gd.pid configuration attribute. You cannot perform any GoodData operations.';
        } elseif (empty($result['gd']['username'])) {
            $error = 'The writer is missing gd.username configuration attribute. You cannot perform any GoodData operations.';
        } elseif (empty($result['gd']['password'])) {
            $error = 'The writer is missing gd.password configuration attribute. You cannot perform any GoodData operations.';
        }

        if ($error) {
            $result['status'] = SharedStorage::WRITER_STATUS_ERROR;
            $result['info'] = $error;
        }

        if (!isset($result['id'])) {
            $result['id'] = $result['writerId'];
        }
        return $result;
    }

    public function getBucketAttributes($checkError = true)
    {
        try {
            $bucket = $this->cachedClient->getBucket($this->bucketId);
            $attributes = self::parseBucketAttributes($bucket['attributes']);
            if ($checkError) {
                if (!empty($attributes['error'])) {
                    throw new UserException($attributes['error']);
                }
            }
            return $attributes;
        } catch (ClientException $e) {
            if ($e->getCode() == 404) {
                throw new UserException('Configuration bucket for \'' . $this->bucketId . '\' is missing');
            } else {
                throw $e;
            }
        }
    }

    /**
     * Update writer's configuration
     */
    public function updateBucketAttribute($key, $value = null, $protected = null)
    {
        if ($value !== null) {
            $this->cachedClient->getClient()->setBucketAttribute($this->bucketId, $key, $value, $protected);
        } else {
            $this->cachedClient->getClient()->deleteBucketAttribute($this->bucketId, $key);
        }
        $this->cachedClient->clearCache();
    }

    /**
     * Delete writer configuration from SAPI
     */
    public function deleteBucket()
    {
        foreach ($this->cachedClient->listTables($this->bucketId) as $table) {
            $this->cachedClient->getClient()->dropTable($table['id']);
        }
        $this->cachedClient->getClient()->dropBucket($this->bucketId);
    }

    public function getWriterToApi()
    {
        $attributes = $this->getBucketAttributes(false);
        try {
            $sharedData = $this->sharedStorage->getWriter($this->projectId, $this->writerId);
            unset($sharedData['feats']);
        } catch (SharedStorageException $e) {
            throw new UserException('Parameter \'writerId\' does not correspond with any configured writer');
        }
        return array_merge($sharedData, $attributes);
    }


    public function getWritersToApi()
    {
        $buckets = [];
        foreach ($this->cachedClient->listBuckets() as $bucket) {
            $buckets[$bucket['id']] = $bucket;
        }

        $result = [];
        foreach ($this->sharedStorage->getActiveWriters($this->projectId) as $writerData) {
            unset($writerData['feats']);
            if (isset($buckets[$writerData['bucket']])) {
                $bucket = $buckets[$writerData['bucket']];
                $attributes = self::parseBucketAttributes($bucket['attributes']);
                $result[] = array_merge($writerData, $attributes);
            }

        }
        return $result;
    }



    /********************
     ********************
     * @section SAPI tables
     ********************/


    /**
     * Get output tables from SAPI
     */
    public function getOutputSapiTables()
    {
        $result = [];
        foreach ($this->cachedClient->listTables() as $table) {
            if (substr($table['id'], 0, 4) == 'out.') {
                $result[] = $table['id'];
            }
        }
        return $result;
    }


    /**
     * Get info about table in SAPI
     */
    public function getSapiTable($tableId)
    {
        try {
            return $this->cachedClient->getTable($tableId);
        } catch (ClientException $e) {
            throw new UserException("Table '$tableId' does not exist or is not accessible with the SAPI token");
        }
    }



    /********************
     ********************
     * @section Data sets
     ********************/


    /**
     * Check output tables and update configuration according to them
     * Remove config of deleted tables and add newly added tables
     */
    public function updateDataSetsFromSapi()
    {
        // Do only once per request
        $cacheKey = 'updateDataSetsFromSapi';
        if (!empty($this->cache[$cacheKey])) {
            return;
        }

        $tableId = $this->bucketId . '.' . self::DATA_SETS_TABLE_NAME;
        if (!$this->cachedClient->tableExists($tableId)) {
            $this->createTable(self::DATA_SETS_TABLE_NAME);
        }

        $outputTables = $this->getOutputSapiTables();
        $configuredTables = [];
        foreach ($this->fetchTable(self::DATA_SETS_TABLE_NAME) as $row) {
            if (!isset($row['id'])) {
                throw new UserException('Configuration table ' . $tableId . ' is missing column id');
            }
            if (!in_array($row['id'], $configuredTables)) {
                $configuredTables[] = $row['id'];
            }
        }

        // Add tables without configuration
        $add = [];
        foreach ($outputTables as $tableId) {
            if (!in_array($tableId, $configuredTables)) {
                $add[] = ['id' => $tableId];
            }
        }
        if (count($add)) {
            $this->saveTable(self::DATA_SETS_TABLE_NAME, $add, true);
        }

        $this->cache[$cacheKey] = true;
    }


    /**
     * Get complete data set definition
     */
    public function getDataSetForApi($tableId)
    {
        $dataSet = $this->getDataSet($tableId);

        $columns = [];
        $sourceTable = $this->getSapiTable($tableId);
        foreach ($sourceTable['columns'] as $columnName) {
            $column = $dataSet['columns'][$columnName];
            $column['name'] = $columnName;
            if (empty($column['gdName'])) {
                $column['gdName'] = $columnName;
            }
            $column = $this->cleanColumnDefinition($column);
            $columns[] = $column;
        }

        return [
            'id' => $tableId,
            'name' => empty($dataSet['name']) ? $tableId : $dataSet['name'], //TODO remove
            'export' => (bool)$dataSet['export'],
            'isExported' => (bool)$dataSet['isExported'],
            'lastChangeDate' => $dataSet['lastChangeDate'] ? $dataSet['lastChangeDate'] : null,
            'incrementalLoad' => $dataSet['incrementalLoad'] ? (int)$dataSet['incrementalLoad'] : false,
            'ignoreFilter' => (bool)$dataSet['ignoreFilter'],
            'columns' => $columns
        ];
    }


    /**
     * Get list of defined data sets
     */
    public function getDataSets()
    {
        $this->updateDataSetsFromSapi();

        $outputTables = $this->getOutputSapiTables();
        $result = [];
        foreach ($this->fetchTable(self::DATA_SETS_TABLE_NAME) as $table) {
            if (in_array($table['id'], $outputTables)) {
                $title = empty($table['name']) ? $table['id'] : $table['name'];
                $result[] = [
                    'id' => $table['id'],
                    'bucket' => substr($table['id'], 0, strrpos($table['id'], '.')),
                    'name' => $title,
                    'title' => $title,
                    'export' => (bool)$table['export'],
                    'isExported' => (bool)$table['isExported'],
                    'lastChangeDate' => $table['lastChangeDate'],
                    'incrementalLoad' => $table['incrementalLoad'] ? (int)$table['incrementalLoad'] : false,
                    'ignoreFilter' => (bool)$table['ignoreFilter'],
                    'identifier' => !empty($table['identifier']) ? $table['identifier'] : Model::getDatasetId($title)
                ];
            }
        }
        return $result;
    }


    /**
     * Get list of defined data sets with connection point
     */
    public function getDataSetsWithConnectionPoint()
    {
        $this->updateDataSetsFromSapi();

        $tables = [];
        foreach ($this->fetchTable(self::DATA_SETS_TABLE_NAME) as $table) {
            $hasConnectionPoint = false;
            if (!empty($table['definition'])) {
                $tableDefinition = json_decode($table['definition'], true);
                if ($tableDefinition === null) {
                    throw new UserException(sprintf("Definition of columns for table '%s' is not valid json", $table['id']));
                }
                foreach ($tableDefinition as $column) {
                    if ($column['type'] == 'CONNECTION_POINT') {
                        $hasConnectionPoint = true;
                        break;
                    }
                }
                if ($hasConnectionPoint) {
                    $tables[$table['id']] = $table['name'] ? $table['name'] : $table['id'];
                }
            }
        }
        return $tables;
    }


    /**
     * Get definition of data set
     */
    public function getDataSet($tableId)
    {
        $this->updateDataSetFromSapi($tableId);

        $data = $this->fetchTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
        if (!$data) {
            throw new UserException("Definition for table '$tableId' does not exist");
        }

        if ($data['definition']) {
            $data['columns'] = json_decode($data['definition'], true);
            if ($data['columns'] === null) {
                throw new UserException("Definition of columns is not valid json");
            }
        } else {
            $data['columns'] = [];
        }
        $data['title'] = !empty($data['name']) ? $data['name'] : $data['id'];
        if (empty($data['identifier'])) {
            $data['identifier'] = Model::getDatasetId($data['title']);
        }

        return $data;
    }



    /**
     * Check if data set has connection point
     */
    public function getDimensionsOfDataSet($tableId)
    {
        $dataSet = $this->getDataSet($tableId);

        $dimensions = [];
        foreach ($dataSet['columns'] as $column) {
            if ($column['type'] == 'DATE' && !empty($column['dateDimension'])) {
                $dimensions[] = $column['dateDimension'];
            }
        }
        return $dimensions;
    }


    /**
     * Update definition of column of a data set
     */
    public function updateColumnsDefinition($tableId, $column, $data = null)
    {
        $this->updateDataSetFromSapi($tableId);

        $tableRow = $this->fetchTableRow(self::DATA_SETS_TABLE_NAME, $tableId, false);
        if (!$tableRow) {
            throw new UserException("Definition for table '$tableId' does not exist");
        }
        if ($tableRow['definition']) {
            $definition = json_decode($tableRow['definition'], true);
            if ($definition === null) {
                throw new UserException("Definition of columns for table '$tableId' is not valid json");
            }
        } else {
            $definition = [];
        }

        if (is_array($column)) {
            // Update more columns
            foreach ($column as $columnData) {
                if (!isset($columnData['name'])) {
                    throw new UserException("One of the columns is missing 'name' parameter");
                }
                $columnName = $columnData['name'];
                unset($columnData['name']);

                foreach (array_keys($columnData) as $key) {
                    if (!in_array($key, self::$columnDefinitions)) {
                        throw new UserException(sprintf("Parameter '%s' is not valid for column definition", $key));
                    }
                }

                $definition[$columnName] = isset($definition[$columnName]) ? array_merge($definition[$columnName], $columnData) : $columnData;
                $definition[$columnName] = $this->cleanColumnDefinition($definition[$columnName]);
            }
        } else {
            // Update one column
            if (!$data) {
                $data = [];
            }
            $definition[$column] = isset($definition[$column]) ? array_merge($definition[$column], $data) : $data;
            $definition[$column] = $this->cleanColumnDefinition($definition[$column]);
        }

        $tableRow['definition'] = json_encode($definition);
        $tableRow['lastChangeDate'] = date('c');
        $this->updateTableRow(self::DATA_SETS_TABLE_NAME, $tableRow);
    }


    /**
     * Remove non-sense definitions
     */
    private function cleanColumnDefinition($data)
    {
        if (empty($data['type'])) {
            $data['type'] = 'IGNORE';
        }
        if (($data['type'] != 'ATTRIBUTE' && $data['type'] != 'CONNECTION_POINT') && isset($data['sortLabel'])) {
            unset($data['sortLabel']);
        }
        if (($data['type'] != 'ATTRIBUTE' && $data['type'] != 'CONNECTION_POINT') && isset($data['sortOrder'])) {
            unset($data['sortOrder']);
        }
        if ($data['type'] != 'REFERENCE' && isset($data['schemaReference'])) {
            unset($data['schemaReference']);
        }
        if (!in_array($data['type'], ['HYPERLINK', 'LABEL']) && isset($data['reference'])) {
            unset($data['reference']);
        }
        if ($data['type'] != 'DATE' && isset($data['format'])) {
            unset($data['format']);
        }
        if ($data['type'] != 'DATE' && isset($data['dateDimension'])) {
            unset($data['dateDimension']);
        }
        if (empty($data['dataTypeSize'])) {
            unset($data['dataTypeSize']);
        }
        if (empty($data['dataType'])) {
            unset($data['dataType']);
        }
        if (empty($data['sortLabel'])) {
            unset($data['sortLabel']);
        }
        if (empty($data['sortOrder'])) {
            unset($data['sortOrder']);
        }
        if ($data['type'] == 'IGNORE') {
            unset($data['schemaReference']);
            unset($data['reference']);
            unset($data['format']);
            unset($data['dateDimension']);
            unset($data['dataType']);
            unset($data['dataTypeSize']);
            unset($data['sortLabel']);
            unset($data['sortOrder']);
        }
        return $data;
    }


    /**
     * Update definition of data set
     */
    public function updateDataSetDefinition($tableId, $name, $value = null)
    {
        $this->updateDataSetFromSapi($tableId);

        $tableRow = $this->fetchTableRow(self::DATA_SETS_TABLE_NAME, $tableId, false);
        if (!$tableRow) {
            throw new UserException("Definition for table '$tableId' does not exist");
        }

        $allowedParams = $this->tables[Configuration::DATA_SETS_TABLE_NAME]['columns'];
        unset($allowedParams['id']);
        unset($allowedParams['lastChangeDate']);
        unset($allowedParams['definition']);

        $tableRow = [];
        if (is_array($name)) {
            unset($name['writerId']);
            // Update more values at once
            foreach (array_keys($name) as $key) {
                if (!in_array($key, $allowedParams)) {
                    throw new UserException(sprintf("Parameter '%s' is not valid for table definition", $key));
                }
            }
            $tableRow = $name;
        } else {
            // Update one value
            if (!in_array($name, $allowedParams)) {
                throw new UserException(sprintf("Parameter '%s' is not valid for table definition", $name));
            }
            $tableRow[$name] = $value;
        }

        $tableRow['id'] = $tableId;
        $tableRow['lastChangeDate'] = date('c');
        $this->updateTableRow(self::DATA_SETS_TABLE_NAME, $tableRow);
    }


    /**
     * Delete definition for columns removed from data table
     */
    public function updateDataSetFromSapi($tableId)
    {
        // Do only once per request
        $cacheKey = 'updateDataSetFromSapi.' . $tableId;
        if (!empty($this->cache[$cacheKey])) {
            return;
        }

        $anythingChanged = false;
        $table = $this->getSapiTable($tableId);
        $dataSet = $this->fetchTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
        if (!$dataSet) {
            $dataSet = array_fill_keys($this->tables[self::DATA_SETS_TABLE_NAME]['columns'], null);
            $dataSet['id'] = $tableId;
            $dataSet['definition'] = [];
            $anythingChanged = true;
        }
        if ($dataSet['definition']) {
            $definition = json_decode($dataSet['definition'], true);
            if ($definition === null) {
                throw new UserException("Definition of columns for table '$tableId' is not valid json");
            }

            // Remove definitions of non-existing columns
            foreach (array_keys($definition) as $definedColumn) {
                if (!in_array($definedColumn, $table['columns'])) {
                    unset($definition[$definedColumn]);
                    $anythingChanged = true;
                }
            }
        } else {
            $definition = [];
        }

        // Added definitions for new columns
        foreach ($table['columns'] as $column) {
            if (!in_array($column, array_keys($definition))) {
                $definition[$column] = ['type' => 'IGNORE'];
                $anythingChanged = true;
            }
        }

        if ($anythingChanged) {
            $dataSet['definition'] = json_encode($definition);
            $dataSet['lastChangeDate'] = date('c');
            $this->updateTableRow(self::DATA_SETS_TABLE_NAME, $dataSet);
        }

        $this->cache[$cacheKey] = true;
    }


    public function getDataSetDefinition($tableId)
    {
        $this->updateDataSetsFromSapi();
        $this->updateDataSetFromSapi($tableId);

        $gdDefinition = $this->getDataSet($tableId);
        $dateDimensions = null; // fetch only when needed
        $sourceTable = $this->getSapiTable($tableId);

        $result = [
            'tableId' => $tableId,
            'title' => $gdDefinition['title'],
            'identifier' => $gdDefinition['identifier'],
            'columns' => []
        ];

        foreach ($sourceTable['columns'] as $columnName) {
            if (!isset($gdDefinition['columns'][$columnName])) {
                $columnDefinition = [
                    'name' => $columnName,
                    'type' => 'IGNORE',
                    'gdName' => $columnName,
                    'dataType' => ''
                ];
            } else {
                $columnDefinition = $gdDefinition['columns'][$columnName];
            }

            $column = [
                'name' => $columnName,
                'title' => (!empty($columnDefinition['gdName']) ? $columnDefinition['gdName'] : $columnName) . ' (' . $gdDefinition['title'] . ')',
                'type' => !empty($columnDefinition['type']) ? $columnDefinition['type'] : 'IGNORE'
            ];
            if (!empty($columnDefinition['dataType'])) {
                $dataType = $columnDefinition['dataType'];
                if (!empty($columnDefinition['dataTypeSize'])) {
                    $dataType .= '(' . $columnDefinition['dataTypeSize'] . ')';
                }
                $column['dataType'] = $dataType;
            }

            if (!empty($columnDefinition['type'])) {
                switch($columnDefinition['type']) {
                    case 'CONNECTION_POINT':
                    case 'ATTRIBUTE':
                        if (!empty($columnDefinition['sortLabel'])) {
                            $column['sortLabel'] = $columnDefinition['sortLabel'];
                            $column['sortOrder'] = !empty($columnDefinition['sortOrder']) ? $columnDefinition['sortOrder'] : 'ASC';
                        }
                        break;
                    case 'LABEL':
                    case 'HYPERLINK':
                        $column['reference'] = $columnDefinition['reference'];
                        break;
                    case 'DATE':
                        if (!$dateDimensions) {
                            $dateDimensions = $this->getDateDimensions();
                        }
                        if (!empty($columnDefinition['dateDimension']) && isset($dateDimensions[$columnDefinition['dateDimension']])) {
                            $column['format'] = $columnDefinition['format'];
                            $column['includeTime'] = (bool)$dateDimensions[$columnDefinition['dateDimension']]['includeTime'];
                            $column['schemaReference'] = $columnDefinition['dateDimension'];
                            if (!empty($dateDimensions[$columnDefinition['dateDimension']]['template'])) {
                                $column['template'] = $dateDimensions[$columnDefinition['dateDimension']]['template'];
                            }
                        } else {
                            throw new UserException("Date column '{$columnName}' does not have valid date dimension assigned");
                        }
                        break;
                    case 'REFERENCE':
                        if ($columnDefinition['schemaReference']) {
                            try {
                                $refTableDefinition = $this->getDataSet($columnDefinition['schemaReference']);
                            } catch (UserException $e) {
                                throw new UserException("Schema reference '{$columnDefinition['schemaReference']}'"
                                    . " of column '{$columnName}' does not exist");
                            }
                            if ($refTableDefinition) {
                                $column['schemaReference'] = $refTableDefinition['title'];
                                $column['schemaReferenceId'] = $refTableDefinition['id'];
                                $reference = null;
                                foreach ($refTableDefinition['columns'] as $cName => $c) {
                                    if ($c['type'] == 'CONNECTION_POINT') {
                                        $reference = $cName;
                                        break;
                                    }
                                }
                                if ($reference) {
                                    $column['reference'] = $reference;
                                } else {
                                    throw new UserException("Schema reference '{$columnDefinition['schemaReference']}' "
                                        . "of column '{$columnName}' does not have connection point");
                                }
                            } else {
                                throw new UserException("Schema reference '{$columnDefinition['schemaReference']}' "
                                    . " of column '{$columnName}' does not exist");
                            }
                        } else {
                            throw new UserException("Schema reference of column '{$columnName}' is empty");
                        }

                        break;
                }
            }
            $result['columns'][] = $column;
        }

        return $result;
    }

    /**
     * Return data sets sorted according to their references
     */
    public function getSortedDataSets()
    {
        $dataSets = [];
        foreach ($this->getDataSets() as $dataSet) {
            if (!empty($dataSet['export'])) {
                try {
                    $definition = $this->getDataSetDefinition($dataSet['id']);
                } catch (UserException $e) {
                    throw new UserException(sprintf('Wrong configuration of table \'%s\': %s', $dataSet['id'], $e->getMessage()));
                }

                $dataSets[$dataSet['id']] = [
                    'tableId' => $dataSet['id'],
                    'title' => $definition['title'],
                    'definition' => $definition,
                    'lastChangeDate' => $dataSet['lastChangeDate'],
                    'identifier' => $dataSet['identifier']
                ];
            }
        }

        // Sort tables for GD export according to their references
        $unsorted = [];
        $sorted = [];
        $references = [];
        $allIds = array_keys($dataSets);
        foreach ($dataSets as $tableId => $tableConfig) {
            $unsorted[$tableId] = $tableConfig;
            foreach ($tableConfig['definition']['columns'] as $c) {
                if ($c['type'] == 'REFERENCE' && !empty($c['schemaReferenceId'])) {
                    if (in_array($c['schemaReferenceId'], $allIds)) {
                        $references[$tableId][] = $c['schemaReferenceId'];
                    } else {
                        throw new UserException("Schema reference '{$c['schemaReferenceId']}' for table '{$tableId}' is not in tables to export");
                    }
                }
            }
        }

        $ttl = 20;
        while (count($unsorted)) {
            foreach ($unsorted as $tableId => $tableConfig) {
                $areSortedReferences = true;
                if (isset($references[$tableId])) {
                    foreach ($references[$tableId] as $r) {
                        if (!array_key_exists($r, $sorted)) {
                            $areSortedReferences = false;
                        }
                    }
                }
                if ($areSortedReferences) {
                    $sorted[$tableId] = $tableConfig;
                    unset($unsorted[$tableId]);
                }
            }
            $ttl--;

            if ($ttl <= 0) {
                throw new UserException('Check of references failed with timeout. You probably have a recursion in references');
            }
        }

        return $sorted;
    }



    /********************
     ********************
     * @section Date dimensions
     ********************/


    /**
     * Get defined date dimensions
     */
    public function getDateDimensions($usage = false)
    {
        if ($usage) {
            return $this->getDateDimensionsWithUsage();
        }

        $tableId = $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME;
        if (!$this->cachedClient->tableExists($tableId)) {
            $this->createTable(self::DATE_DIMENSIONS_TABLE_NAME);
            return [];
        } else {
            $data = [];
            foreach ($this->fetchTable(self::DATE_DIMENSIONS_TABLE_NAME) as $row) {
                $row['includeTime'] = (bool)$row['includeTime'];
                $row['isExported'] = (bool)$row['isExported'];
                $data[$row['name']] = $row;
            }

            return $data;
        }
    }


    /**
     * Get defined date dimensions with usage in data sets
     */
    public function getDateDimensionsWithUsage()
    {
        $dimensions = $this->getDateDimensions();

        $usage = [];
        foreach ($this->getDataSets() as $dataSet) {
            foreach ($this->getDimensionsOfDataSet($dataSet['id']) as $dimension) {
                if (!isset($usage[$dimension])) {
                    $usage[$dimension]['usedIn'] = [];
                }
                $usage[$dimension]['usedIn'][] = $dataSet['id'];
            }
        }

        return array_merge_recursive($dimensions, $usage);
    }


    /**
     * Add date dimension
     */
    public function saveDateDimension($name, $includeTime = false, $template = null)
    {
        $data = [
            'name' => $name,
            'includeTime' => $includeTime,
            'template' => $template,
            'isExported' => null
        ];
        $this->updateTableRow(self::DATE_DIMENSIONS_TABLE_NAME, $data);
    }

    public function setDateDimensionIsExported($name, $isExported = true)
    {
        $data = [
            'name' => $name,
            'isExported' => $isExported? 1 : 0
        ];
        $this->updateTableRow(self::DATE_DIMENSIONS_TABLE_NAME, $data);
    }

    /**
     * Delete date dimension
     */
    public function deleteDateDimension($name)
    {
        $this->cachedClient->deleteTableRows($this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME, 'name', $name);
    }




    /********************
     ********************
     * @section Project clones
     ********************/


    /**
     * Get list of all projects
     */
    public function getProjects()
    {
        $bucketAttributes = $this->getBucketAttributes();
        $projects = $this->fetchTable(self::PROJECTS_TABLE_NAME);
        if (isset($bucketAttributes['gd']['pid'])) {
            array_unshift($projects, ['pid' => $bucketAttributes['gd']['pid'], 'active' => true, 'main' => true]);
        }
        return $projects;
    }


    /**
     * Get project if exists
     * @return array|bool
     */
    public function getProject($pid)
    {
        foreach ($this->getProjects() as $project) {
            if ($project['pid'] == $pid) {
                return $project;
            }
        }
        return false;
    }

    public function resetProjectsTable()
    {
        $this->truncateTable(self::PROJECTS_TABLE_NAME);
    }


    /**
     *
     */
    public function saveProject($pid)
    {
        $data = [
            'pid' => $pid,
            'active' => 1
        ];
        $this->updateTableRow(self::PROJECTS_TABLE_NAME, $data);
    }



    /********************
     ********************
     * @section Project users
     ********************/


    /**
     * Get list of all users
     */
    public function getUsers()
    {
        $bucketAttributes = $this->getBucketAttributes();
        $users = $this->fetchTable(self::USERS_TABLE_NAME);
        if (isset($bucketAttributes['gd']['username']) && isset($bucketAttributes['gd']['uid'])) {
            array_unshift($users, [
                'email' => $bucketAttributes['gd']['username'],
                'uid' => $bucketAttributes['gd']['uid'],
                'main' => true
            ]);
        }
        return $users;
    }


    /**
     * Get user if exists
     * @return array|bool
     */
    public function getUser($email)
    {
        foreach ($this->getUsers() as $user) {
            if (strtolower($user['email']) == strtolower($email)) {
                return $user;
            }
        }
        return false;
    }

    /**
     * Check if user was invited/added to project by writer
     */
    public function isProjectUser($email, $pid)
    {
        foreach ($this->getProjectUsers() as $projectUser) {
            if (strtolower($projectUser['email']) == strtolower($email) && $projectUser['pid'] == $pid && empty($projectUser['main'])) {
                return true;
            }
        }

        return false;
    }

    /**
     *
     */
    public function getProjectUsers($pid = null)
    {
        $bucketAttributes = $this->getBucketAttributes();
        $projectUsers = $this->fetchTable(self::PROJECT_USERS_TABLE_NAME);
        if (!count($projectUsers) && isset($bucketAttributes['gd']['pid']) && isset($bucketAttributes['gd']['username'])) {
            array_unshift($projectUsers, [
                'id' => 0,
                'pid' => $bucketAttributes['gd']['pid'],
                'email' => $bucketAttributes['gd']['username'],
                'role' => 'admin',
                'action' => 'add',
                'main' => true
            ]);
        }

        if ($pid) {
            $result = [];
            foreach ($projectUsers as $u) {
                if ($u['pid'] == $pid) {
                    $result[] = [
                        'email' => $u['email'],
                        'role' => $u['role']
                    ];
                }
            }
            return $result;
        }

        return $projectUsers;
    }

    /**
     * Save user to configuration
     */
    public function saveUser($email, $uid)
    {
        $data = [
            'email' => strtolower($email),
            'uid' => $uid
        ];
        $this->updateTableRow(self::USERS_TABLE_NAME, $data);
    }


    /**
     * Save project user to configuration
     */
    public function saveProjectUser($pid, $email, $role, $invite = false)
    {
        $action = $invite? 'invite' : 'add';
        $data = [
            'id' => sha1($pid . strtolower($email) . $action . date('c')),
            'pid' => $pid,
            'email' => strtolower($email),
            'role' => $role,
            'action' => $action
        ];
        $this->updateTableRow(self::PROJECT_USERS_TABLE_NAME, $data);
    }

    /**
     *
     */
    public function deleteProjectUser($pid, $email)
    {
        $filter = [];
        foreach ($this->getProjectUsers() as $projectUser) {
            if (isset($projectUser['main'])) {
                throw new UserException('Main user cannot be removed from main project');
            }

            if ($projectUser['pid'] == $pid && strtolower($projectUser['email']) == strtolower($email)) {
                $filter[] = $projectUser['id'];
            }
        }

        if (!$filter) {
            return;
        }

        $this->cachedClient->deleteTableRows($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, 'id', $filter);
    }




    /********************
     ********************
     * @section Filters
     ********************/

    /**
     * Get all filters
     */
    public function getFilters($names = [])
    {
        $filters = count($names)? $this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_TABLE_NAME, 'name', $names)
            : $this->fetchTable(self::FILTERS_TABLE_NAME);
        foreach ($filters as &$filter) {
            if (in_array(substr($filter['value'], 0, 1), ['[', '{'])) {
                $filter['value'] = json_decode($filter['value'], true);
            }
        }
        return $filters;
    }
    /**
     * Get filter by name
     */
    public function getFilter($name)
    {
        $filters = $this->getFilters([$name]);
        return count($filters)? end($filters) : false;
    }
    /**
     * Get filters by email
     */
    public function getFiltersForUser($email)
    {
        $filters = [];
        foreach ($this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'email', $email) as $fu) {
            $filters[] = $fu['filter'];
        }

        return count($filters)? $this->getFilters($filters) : [];
    }
    /**
     * Get filters by pid
     */
    public function getFiltersForProject($pid)
    {
        $filters = [];
        foreach ($this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'pid', $pid) as $fp) {
            $filters[] = $fp['filter'];
        }

        return count($filters)? $this->getFilters($filters) : [];
    }


    /**
     * Get all filters_projects
     */
    public function getFiltersProjects()
    {
        return $this->fetchTable(self::FILTERS_PROJECTS_TABLE_NAME);
    }
    /**
     * Get filters_projects by pid
     */
    public function getFiltersProjectsByPid($pid)
    {
        return $this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'pid', $pid);
    }
    /**
     * Get filters_projects by filter
     */
    public function getFiltersProjectsByFilter($filter)
    {
        return $this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'filter', $filter);
    }


    /**
     * Get all filters_users
     */
    public function getFiltersUsers()
    {
        return $this->fetchTable(self::FILTERS_USERS_TABLE_NAME);
    }
    /**
     * Get filters_users by email
     */
    public function getFiltersUsersByEmail($email)
    {
        return $this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'email', $email);
    }
    /**
     * Get filters_users by filter
     */
    public function getFiltersUsersByFilter($filter)
    {
        return $this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'filter', $filter);
    }


    /**
     * Check if filter uri is in filters_projects table
     * @deprecated Backwards compatibility
     */
    public function checkFilterUri($uri)
    {
        $filters = $this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'uri', $uri);
        return count($filters) > 0;
    }


    /**
     *
     */
    public function saveFilter($name, $attribute, $operator, $value, $over = null, $to = null)
    {
        if ($this->cachedClient->tableExists($this->bucketId . '.' . self::FILTERS_TABLE_NAME) && $this->getFilter($name)) {
            throw new UserException("Filter of that name already exists.");
        }

        $data = [
            'name' => $name,
            'attribute' => $attribute,
            'operator' => $operator,
            'value' => is_array($value)? json_encode($value) : $value,
            'over' => $over,
            'to' => $to
        ];
        $this->updateTableRow(self::FILTERS_TABLE_NAME, $data);
    }

    public function saveFiltersProjects($uri, $filter, $pid)
    {
        if ($this->cachedClient->tableExists($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME)
            && count($this->cachedClient->exportTable($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'uri', $uri))) {
            throw new UserException("Filter is already assigned to the project.");
        }

        $data = [
            'uri' => $uri,
            'filter' => $filter,
            'pid' => $pid
        ];
        $this->updateTableRow(self::FILTERS_PROJECTS_TABLE_NAME, $data);
    }

    /**
     *
     */
    public function saveFiltersUsers(array $filters, $email)
    {
        $this->cachedClient->deleteTableRows($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'email', $email);
        if (count($filters)) {
            $data = [];
            foreach ($filters as $f) {
                $data[] = [
                    'id' => sha1($f . '.' . $email),
                    'filter' => $f,
                    'email' => $email
                ];
            }
            $this->saveTable(self::FILTERS_USERS_TABLE_NAME, $data, true);
        }
    }

    public function deleteFilter($name)
    {
        $this->cachedClient->deleteTableRows($this->bucketId . '.' . self::FILTERS_TABLE_NAME, 'name', $name);
        $this->cachedClient->deleteTableRows($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'filter', $name);
        $this->cachedClient->deleteTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'filter', $name);
    }

    public function deleteFilterFromProject($uri)
    {
        $this->cachedClient->deleteTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'uri', $uri);
    }

    public function getTableIdFromAttribute($attr)
    {
        $attrArray = explode('.', $attr);
        if (count($attrArray) != 4) {
            throw new UserException(sprintf("Attribute parameter '%s' has wrong format", $attr));
        }
        $tableId = sprintf('%s.%s.%s', $attrArray[0], $attrArray[1], $attrArray[2]);

        $sapiTable = $this->getSapiTable($tableId);
        if (!in_array($attrArray[3], $sapiTable['columns'])) {
            throw new UserException(sprintf("Attribute parameter '%s' has wrong format, column '%s' not found in table '%s'", $attr, $attrArray[3], $tableId));
        }

        return $tableId;
    }



    /**
     * Save SAPI table
     */
    protected function saveTable($tableName, array $data = [], $incremental = true, $partial = true)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        return $this->cachedClient->saveTable(
            $this->bucketId . '.' . $tableName,
            count($data) ? array_keys($data[0]) : $this->tables[$tableName]['columns'],
            $data,
            $this->tables[$tableName]['primaryKey'],
            $this->tables[$tableName]['indices'],
            $incremental,
            $partial
        );
    }


    /**
     * Create configuration table
     */
    protected function createTable($tableName)
    {
        if ($this->cachedClient->tableExists($this->bucketId . '.' . $tableName)) {
            return false;
        }

        try {
            return $this->saveTable($tableName, [], false, false);
        } catch (ClientException $e) {
            if (!in_array($e->getCode(), [400, 404])) {
                throw $e;
            }
            return false;
        }
    }

    /**
     * Empty configuration table
     */
    public function truncateTable($tableName)
    {
        return $this->saveTable($tableName, [], false, false);
    }

    /**
     * Update row of configuration table
     */
    protected function updateTableRow($tableName, $data)
    {
        return $this->saveTable($tableName, [$data]);
    }

    public function checkTable($tableName)
    {
        $tableId = $this->bucketId . '.' . $tableName;
        if ($this->cachedClient->tableExists($tableId)) {
            $table = $this->getSapiTable($tableId);
            $this->checkTableConfiguration($tableName, $table['columns']);
        }
    }

    /**
     * Check if configuration table contains all required columns
     */
    protected function checkTableConfiguration($tableName, $columns)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        // Allow tables to have more columns then according to definitions
        if (count(array_diff($this->tables[$tableName]['columns'], $columns))) {
            throw new UserException(sprintf(
                "Table '%s' appears to be wrongly configured. Contains columns: '%s' but should contain columns: '%s'",
                $tableName,
                implode(',', $columns),
                implode(',', $this->tables[$tableName]['columns'])
            ));
        }

        return true;
    }

    /**
     * Get all rows from configuration table
     */
    protected function fetchTable($tableName)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        try {
            $table = $this->cachedClient->exportTable($this->bucketId . '.' . $tableName);
        } catch (ClientException $e) {
            if ($e->getCode() == 404) {
                $this->createTable($tableName);
                $table = [];
            } else {
                throw $e;
            }
        }

        if (count($table)) {
            $this->checkTableConfiguration($tableName, array_keys(current($table)));
        }

        return $table;
    }

    /**
     * Get row from configuration table
     */
    protected function fetchTableRow($tableName, $id, $cache = true)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        $result = $this->cachedClient->exportTable(
            $this->bucketId . '.' . $tableName,
            $this->tables[$tableName]['primaryKey'],
            $id,
            $cache
        );
        return count($result) ? current($result) : false;
    }


    public function migrateDatasets()
    {
        $table = Client::parseCsv($this->cachedClient->getClient()->exportTable($this->bucketId.'.data_sets'));
        $resultTable = [];
        foreach ($table as $row) {
            $dataSetName = !empty($row['name']) ? $row['name'] : $row['id'];
            $result = [
                'tableId' => $row['id'],
                'identifier' => Model::getDatasetId($dataSetName),
                'title' => $dataSetName,
                'export' => $row['export'],
                'isExported' => $row['isExported'],
                'lastChangeDate' => $row['lastChangeDate'],
                'incrementalLoad' => $row['incrementalLoad'],
                'ignoreFilter' => $row['ignoreFilter'],
                'definition' => []
            ];
            $definition = json_decode($row['definition'], true);
            foreach ($definition as $col => $def) {
                switch ($def['type']) {
                    case 'CONNECTION_POINT':
                    case 'ATTRIBUTE':
                        $identifier = Model::getAttributeId($dataSetName, $col);
                        break;
                    case 'FACT':
                        $identifier = Model::getFactId($dataSetName, $col);
                        break;
                    case 'LABEL':
                    case 'HYPERLINK':
                        $identifier = Model::getLabelId($dataSetName, $col);
                        break;
                    default:
                        $identifier = null;
                }
                $result['definition'][$col] = [];
                if (isset($def['type']) && $def['type'] != 'IGNORE') {
                    $result['definition'][$col]['type'] = $def['type'];
                    $result['definition'][$col]['identifier'] = $identifier;
                    $result['definition'][$col]['title'] = (isset($def['gdName']) ? $def['gdName'] : $col).'('.$dataSetName.')';
                }
                if (isset($def['dataType'])) {
                    $result['definition'][$col]['dataType'] = $def['dataType'];
                    if (isset($def['dataTypeSize'])) {
                        $result['definition'][$col]['dataTypeSize'] = $def['dataTypeSize'];
                    }
                }
                if (isset($def['schemaReference'])) {
                    $result['definition'][$col]['schemaReference'] = $def['schemaReference'];
                }
                if (isset($def['reference'])) {
                    $result['definition'][$col]['reference'] = $def['reference'];
                }
                if (isset($def['format'])) {
                    $result['definition'][$col]['format'] = $def['format'];
                }
                if (isset($def['dateDimension'])) {
                    $result['definition'][$col]['dateDimension'] = $def['dateDimension'];
                }
                if (isset($def['sortLabel'])) {
                    $result['definition'][$col]['sortLabel'] = $def['sortLabel'];
                    if (isset($def['sortOrder'])) {
                        $result['definition'][$col]['sortOrder'] = $def['sortOrder'];
                    }
                }
            }
            $result['definition'] = json_encode($result['definition']);
            $resultTable[] = $result;
        }
        $this->cachedClient->getClient()->dropTable($this->bucketId.'.data_sets');
        $this->saveTable('data_sets', $resultTable, false, false);
    }
}
