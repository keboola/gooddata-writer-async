<?php
/**
 * Configuration Wrapper
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\SharedStorageException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\Service\StorageApiConfiguration;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\StorageApi\ClientException;

class Configuration extends StorageApiConfiguration
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


    /**
     * @var SharedStorage
     */
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
    public function __construct(StorageApiClient $storageApiClient, SharedStorage $sharedStorage)
    {
        parent::__construct($storageApiClient);
        $this->sharedStorage = $sharedStorage;

        $logData = $this->storageApiClient->getLogData();
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
        $this->tokenInfo = $this->storageApiClient->getLogData();
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


    /********************
     ********************
     * @section Writer and it's bucket
     ********************/

    /**
     * Create configuration bucket
     */
    public function createBucket($writerId)
    {
        $this->storageApiClient->createBucket('wr-gooddata-' . $writerId, 'sys', 'GoodData Writer Configuration');
        $this->storageApiClient->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'writer', self::WRITER_NAME);
        $this->storageApiClient->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'writerId', $writerId);
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

    public function bucketAttributes($checkError = true)
    {
        try {
            $bucket = $this->storageApiClient->getBucket($this->bucketId);
            $attributes = self::parseBucketAttributes($bucket['attributes']);
            if ($checkError) {
                if (!empty($attributes['error'])) {
                    throw new WrongConfigurationException($attributes['error']);
                }
            }
            return $attributes;
        } catch (ClientException $e) {
            if ($e->getCode() == 404) {
                throw new WrongConfigurationException('Configuration bucket for \'' . $this->bucketId . '\' is missing');
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
            $this->storageApiClient->setBucketAttribute($this->bucketId, $key, $value, $protected);
        } else {
            $this->storageApiClient->deleteBucketAttribute($this->bucketId, $key);
        }
        $this->cache['bucketInfo.' . $this->bucketId][$key] = $value; //@TODO
    }

    /**
     * Delete writer configuration from SAPI
     */
    public function deleteBucket()
    {
        foreach ($this->sapiListTables($this->bucketId) as $table) {
            $this->storageApiClient->dropTable($table['id']);
        }
        $this->storageApiClient->dropBucket($this->bucketId);
    }

    public function getWriterToApi()
    {
        $attributes = $this->bucketAttributes(false);
        try {
            $sharedData = $this->sharedStorage->getWriter($this->projectId, $this->writerId);
            unset($sharedData['feats']);
        } catch (SharedStorageException $e) {
            throw new WrongParametersException('Parameter \'writerId\' does not correspond with any configured writer');
        }
        return array_merge($sharedData, $attributes);
    }


    public function getWritersToApi()
    {
        $buckets = [];
        foreach ($this->sapiListBuckets() as $bucket) {
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
        foreach ($this->sapiListTables() as $table) {
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
            return $this->sapiGetTable($tableId);
        } catch (ClientException $e) {
            throw new WrongConfigurationException("Table '$tableId' does not exist or is not accessible with the SAPI token");
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
        if (!$this->sapiTableExists($tableId)) {
            $this->createConfigTable(self::DATA_SETS_TABLE_NAME);
        }

        $outputTables = $this->getOutputSapiTables();
        $configuredTables = [];
        foreach ($this->fetchTableRows($tableId) as $row) {
            if (!isset($row['id'])) {
                throw new WrongConfigurationException('Configuration table ' . $tableId . ' is missing column id');
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
            $this->updateConfigTable(self::DATA_SETS_TABLE_NAME, $add);
            $this->clearCache();
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
            'name' => empty($dataSet['name']) ? $tableId : $dataSet['name'],
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
        $tables = [];
        foreach ($this->getConfigTable(self::DATA_SETS_TABLE_NAME) as $table) {
            if (in_array($table['id'], $outputTables)) {
                $tables[] = [
                    'id' => $table['id'],
                    'bucket' => substr($table['id'], 0, strrpos($table['id'], '.')),
                    'name' => empty($table['name']) ? $table['id'] : $table['name'],
                    'export' => (bool)$table['export'],
                    'isExported' => (bool)$table['isExported'],
                    'lastChangeDate' => $table['lastChangeDate'],
                    'incrementalLoad' => $table['incrementalLoad'] ? (int)$table['incrementalLoad'] : false,
                    'ignoreFilter' => (bool)$table['ignoreFilter']
                ];
            }
        }
        return $tables;
    }


    /**
     * Get list of defined data sets with connection point
     */
    public function getDataSetsWithConnectionPoint()
    {
        $this->updateDataSetsFromSapi();

        $tables = [];
        foreach ($this->getConfigTable(self::DATA_SETS_TABLE_NAME) as $table) {
            $hasConnectionPoint = false;
            if (!empty($table['definition'])) {
                $tableDefinition = json_decode($table['definition'], true);
                if ($tableDefinition === null) {
                    throw new WrongConfigurationException(sprintf("Definition of columns for table '%s' is not valid json", $table['id']));
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

        $tableConfig = $this->getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
        if (!$tableConfig) {
            throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
        }

        if ($tableConfig['definition']) {
            $tableConfig['columns'] = json_decode($tableConfig['definition'], true);
            if ($tableConfig['columns'] === null) {
                throw new WrongConfigurationException("Definition of columns is not valid json");
            }
        } else {
            $tableConfig['columns'] = [];
        }

        return $tableConfig;
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

        $tableRow = $this->getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId, false);
        if (!$tableRow) {
            throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
        }
        if ($tableRow['definition']) {
            $definition = json_decode($tableRow['definition'], true);
            if ($definition === null) {
                throw new WrongConfigurationException("Definition of columns for table '$tableId' is not valid json");
            }
        } else {
            $definition = [];
        }

        if (is_array($column)) {
            // Update more columns
            foreach ($column as $columnData) {
                if (!isset($columnData['name'])) {
                    throw new WrongParametersException("One of the columns is missing 'name' parameter");
                }
                $columnName = $columnData['name'];
                unset($columnData['name']);

                foreach (array_keys($columnData) as $key) {
                    if (!in_array($key, self::$columnDefinitions)) {
                        throw new WrongParametersException(sprintf("Parameter '%s' is not valid for column definition", $key));
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
        $this->updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableRow);
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
            unset($data['dataTypeSgeize']);
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

        $tableRow = $this->getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId, false);
        if (!$tableRow) {
            throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
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
                    throw new WrongParametersException(sprintf("Parameter '%s' is not valid for table definition", $key));
                }
            }
            $tableRow = $name;
        } else {
            // Update one value
            if (!in_array($name, $allowedParams)) {
                throw new WrongParametersException(sprintf("Parameter '%s' is not valid for table definition", $name));
            }
            $tableRow[$name] = $value;
        }

        $tableRow['id'] = $tableId;
        $tableRow['lastChangeDate'] = date('c');
        $this->updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableRow);
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
        $dataSet = $this->getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
        if (!$dataSet) {
            $dataSet = array_fill_keys($this->tables[self::DATA_SETS_TABLE_NAME]['columns'], null);
            $dataSet['id'] = $tableId;
            $dataSet['definition'] = [];
            $anythingChanged = true;
        }
        if ($dataSet['definition']) {
            $definition = json_decode($dataSet['definition'], true);
            if ($definition === null) {
                throw new WrongConfigurationException("Definition of columns for table '$tableId' is not valid json");
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
            $this->updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $dataSet);
            $this->clearCache();
        }

        $this->cache[$cacheKey] = true;
    }


    public function getDataSetDefinition($tableId)
    {
        $this->updateDataSetsFromSapi();
        $this->updateDataSetFromSapi($tableId);

        $gdDefinition = $this->getDataSet($tableId);
        $dateDimensions = null; // fetch only when needed
        $dataSetName = !empty($gdDefinition['name']) ? $gdDefinition['name'] : $gdDefinition['id'];
        $sourceTable = $this->getSapiTable($tableId);

        $result = [
            'name' => $dataSetName,
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
                'title' => (!empty($columnDefinition['gdName']) ? $columnDefinition['gdName'] : $columnName) . ' (' . $dataSetName . ')',
                //@TODO 'title' => (!empty($columnDefinition['gdName']) ? $columnDefinition['gdName'] : $columnName)
                //@TODO    . (!empty($gdDefinition['addTitleToColumns'])? ' (' . $dataSetName . ')' : ''),
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
                            throw new WrongConfigurationException("Date column '{$columnName}' does not have valid date dimension assigned");
                        }
                        break;
                    case 'REFERENCE':
                        if ($columnDefinition['schemaReference']) {
                            try {
                                $refTableDefinition = $this->getDataSet($columnDefinition['schemaReference']);
                            } catch (WrongConfigurationException $e) {
                                throw new WrongConfigurationException("Schema reference '{$columnDefinition['schemaReference']}'"
                                    . " of column '{$columnName}' does not exist");
                            }
                            if ($refTableDefinition) {
                                $refTableName = !empty($refTableDefinition['name']) ? $refTableDefinition['name'] : $refTableDefinition['id'];
                                $column['schemaReference'] = $refTableName;
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
                                    throw new WrongConfigurationException("Schema reference '{$columnDefinition['schemaReference']}' "
                                        . "of column '{$columnName}' does not have connection point");
                                }
                            } else {
                                throw new WrongConfigurationException("Schema reference '{$columnDefinition['schemaReference']}' "
                                    . " of column '{$columnName}' does not exist");
                            }
                        } else {
                            throw new WrongConfigurationException("Schema reference of column '{$columnName}' is empty");
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
        // Include data set if not excluded and is included or if we do not want included only then look to export flag
        foreach ($this->getDataSets() as $dataSet) {
            if (!empty($dataSet['export'])) {
                try {
                    $definition = $this->getDataSetDefinition($dataSet['id']);
                } catch (WrongConfigurationException $e) {
                    throw new WrongConfigurationException(sprintf('Wrong configuration of table \'%s\': %s', $dataSet['id'], $e->getMessage()));
                }

                $dataSets[$dataSet['id']] = [
                    'tableId' => $dataSet['id'],
                    'title' => $definition['name'],
                    'definition' => $definition,
                    'lastChangeDate' => $dataSet['lastChangeDate']
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
                        throw new WrongConfigurationException("Schema reference '{$c['schemaReferenceId']}' for table '{$tableId}' is not in tables to export");
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
                throw new WrongConfigurationException('Check of references failed with timeout. You probably have a recursion in references');
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
        if (!$this->sapiTableExists($tableId)) {
            $this->createConfigTable(self::DATE_DIMENSIONS_TABLE_NAME);
            return [];
        } else {
            $data = [];
            foreach ($this->getConfigTable(self::DATE_DIMENSIONS_TABLE_NAME) as $row) {
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
        $this->updateConfigTableRow(self::DATE_DIMENSIONS_TABLE_NAME, $data);
    }

    public function setDateDimensionIsExported($name, $isExported = true)
    {
        $data = [
            'name' => $name,
            'isExported' => $isExported? 1 : 0
        ];
        $this->updateConfigTableRow(self::DATE_DIMENSIONS_TABLE_NAME, $data);
        $this->clearCache();
    }

    /**
     * Delete date dimension
     */
    public function deleteDateDimension($name)
    {
        $this->deleteTableRows($this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME, 'name', $name);
        $this->clearCache();
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
        $bucketAttributes = $this->bucketAttributes();
        $projects = $this->getConfigTable(self::PROJECTS_TABLE_NAME);
        if (isset($bucketAttributes['gd']['pid'])) {
            array_unshift($projects, ['pid' => $bucketAttributes['gd']['pid'], 'active' => true, 'main' => true]);
        }
        return $projects;
    }


    /**
     * Get project if exists
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


    /**
     * Check configuration table of projects
     */
    public function checkProjectsTable()
    {
        $tableId = $this->bucketId . '.' . self::PROJECTS_TABLE_NAME;
        if ($this->sapiTableExists($tableId)) {
            $table = $this->getSapiTable($tableId);
            $this->checkConfigTable(self::PROJECTS_TABLE_NAME, $table['columns']);
        }
    }

    public function resetProjectsTable()
    {
        $this->resetConfigTable(self::PROJECTS_TABLE_NAME);
        $this->clearCache();
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
        $this->updateConfigTableRow(self::PROJECTS_TABLE_NAME, $data);
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
        $bucketAttributes = $this->bucketAttributes();
        $users = $this->getConfigTable(self::USERS_TABLE_NAME);
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
     * Check configuration table of users
     */
    public function checkUsersTable()
    {
        $tableId = $this->bucketId . '.' . self::USERS_TABLE_NAME;
        if ($this->sapiTableExists($tableId)) {
            $table = $this->getSapiTable($tableId);
            $this->checkConfigTable(self::USERS_TABLE_NAME, $table['columns']);
        }
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
        $bucketAttributes = $this->bucketAttributes();
        $projectUsers = $this->getConfigTable(self::PROJECT_USERS_TABLE_NAME);
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
     * Check configuration table of users
     */
    public function checkProjectUsersTable()
    {
        $tableId = $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME;
        if ($this->sapiTableExists($tableId)) {
            $table = $this->getSapiTable($tableId);
            $this->checkConfigTable(self::PROJECT_USERS_TABLE_NAME, $table['columns']);
        }
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
        $this->updateConfigTableRow(self::USERS_TABLE_NAME, $data);
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
        $this->updateConfigTableRow(self::PROJECT_USERS_TABLE_NAME, $data);
    }

    /**
     *
     */
    public function deleteProjectUser($pid, $email)
    {
        $filter = [];
        foreach ($this->getProjectUsers() as $projectUser) {
            if (isset($projectUser['main'])) {
                throw new WrongConfigurationException('Main user cannot be removed from main project');
            }

            if ($projectUser['pid'] == $pid && strtolower($projectUser['email']) == strtolower($email)) {
                $filter[] = $projectUser['id'];
            }
        }

        if (!$filter) {
            return;
        }

        $this->storageApiClient->deleteTableRows(
            $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME,
            [
                'whereColumn' => 'id',
                'whereValues' => $filter,
            ]
        );
        $this->clearCache();
    }




    /********************
     ********************
     * @section Filters
     ********************/

    public function checkFiltersTable()
    {
        $tableId = $this->bucketId . '.' . self::FILTERS_TABLE_NAME;
        if ($this->sapiTableExists($tableId)) {
            $table = $this->getSapiTable($tableId);
            $this->checkConfigTable(self::FILTERS_TABLE_NAME, $table['columns']);
        }
    }



    /**
     * Get all filters
     */
    public function getFilters($names = [])
    {
        $filters = count($names)? $this->fetchTableRows($this->bucketId . '.' . self::FILTERS_TABLE_NAME, 'name', $names)
            : $this->getConfigTable(self::FILTERS_TABLE_NAME);
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
        foreach ($this->fetchTableRows($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'email', $email) as $fu) {
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
        foreach ($this->fetchTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'pid', $pid) as $fp) {
            $filters[] = $fp['filter'];
        }

        return count($filters)? $this->getFilters($filters) : [];
    }


    /**
     * Get all filters_projects
     */
    public function getFiltersProjects()
    {
        return $this->getConfigTable(self::FILTERS_PROJECTS_TABLE_NAME);
    }
    /**
     * Get filters_projects by pid
     */
    public function getFiltersProjectsByPid($pid)
    {
        return $this->fetchTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'pid', $pid);
    }
    /**
     * Get filters_projects by filter
     */
    public function getFiltersProjectsByFilter($filter)
    {
        return $this->fetchTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'filter', $filter);
    }


    /**
     * Get all filters_users
     */
    public function getFiltersUsers()
    {
        return $this->getConfigTable(self::FILTERS_USERS_TABLE_NAME);
    }
    /**
     * Get filters_users by email
     */
    public function getFiltersUsersByEmail($email)
    {
        return $this->fetchTableRows($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'email', $email);
    }
    /**
     * Get filters_users by filter
     */
    public function getFiltersUsersByFilter($filter)
    {
        return $this->fetchTableRows($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'filter', $filter);
    }


    /**
     * Check if filter uri is in filters_projects table
     * @deprecated Backwards compatibility
     */
    public function checkFilterUri($uri)
    {
        $filters = $this->fetchTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'uri', $uri);
        return count($filters) > 0;
    }


    /**
     *
     */
    public function saveFilter($name, $attribute, $operator, $value, $over = null, $to = null)
    {
        if ($this->sapiTableExists($this->bucketId . '.' . self::FILTERS_TABLE_NAME) && $this->getFilter($name)) {
            throw new WrongParametersException("Filter of that name already exists.");
        }

        $data = [
            'name' => $name,
            'attribute' => $attribute,
            'operator' => $operator,
            'value' => is_array($value)? json_encode($value) : $value,
            'over' => $over,
            'to' => $to
        ];
        $this->updateConfigTableRow(self::FILTERS_TABLE_NAME, $data);
    }

    public function saveFiltersProjects($uri, $filter, $pid)
    {
        if ($this->sapiTableExists($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME)
            && count($this->fetchTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'uri', $uri))) {
            throw new WrongParametersException("Filter is already assigned to the project.");
        }

        $data = [
            'uri' => $uri,
            'filter' => $filter,
            'pid' => $pid
        ];
        $this->updateConfigTableRow(self::FILTERS_PROJECTS_TABLE_NAME, $data);
    }

    /**
     *
     */
    public function saveFiltersUsers(array $filters, $email)
    {
        $this->deleteTableRows($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'email', $email);
        if (count($filters)) {
            $data = [];
            foreach ($filters as $f) {
                $data[] = [
                    'id' => sha1($f . '.' . $email),
                    'filter' => $f,
                    'email' => $email
                ];
            }
            $this->updateConfigTable(self::FILTERS_USERS_TABLE_NAME, $data);
        }
    }

    public function deleteFilter($name)
    {
        $this->deleteTableRows($this->bucketId . '.' . self::FILTERS_TABLE_NAME, 'name', $name);
        $this->deleteTableRows($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME, 'filter', $name);
        $this->deleteTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'filter', $name);
        $this->clearCache();
    }

    public function deleteFilterFromProject($uri)
    {
        $this->deleteTableRows($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME, 'uri', $uri);
        $this->clearCache();
    }

    public function getTableIdFromAttribute($attr)
    {
        $attrArray = explode('.', $attr);
        if (count($attrArray) != 4) {
            throw new WrongConfigurationException(sprintf("Attribute parameter '%s' has wrong format", $attr));
        }
        $tableId = sprintf('%s.%s.%s', $attrArray[0], $attrArray[1], $attrArray[2]);

        $sapiTable = $this->getSapiTable($tableId);
        if (!in_array($attrArray[3], $sapiTable['columns'])) {
            throw new WrongParametersException(sprintf("Attribute parameter '%s' has wrong format, column '%s' not found in table '%s'", $attr, $attrArray[3], $tableId));
        }

        return $tableId;
    }
}
