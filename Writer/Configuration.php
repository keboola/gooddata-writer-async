<?php
/**
 * Configuration Wrapper
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\Service\StorageApiConfiguration, 
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException;
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
	public $tables = array(
		self::PROJECTS_TABLE_NAME => array(
			'columns' => array('pid', 'active'),
			'primaryKey' => 'pid',
			'indices' => array()
		),
		self::USERS_TABLE_NAME => array(
			'columns' => array('email', 'uid'),
			'primaryKey' => 'email',
			'indices' => array()
		),
		self::PROJECT_USERS_TABLE_NAME => array(
			'columns' => array('id', 'pid', 'email', 'role', 'action'),
			'primaryKey' => 'id',
			'indices' => array('pid', 'email')
		),
		self::FILTERS_TABLE_NAME => array(
			'columns' => array('name', 'attribute', 'element', 'operator', 'uri'),
			'primaryKey' => 'name',
			'indices' => array()
		),
		self::FILTERS_USERS_TABLE_NAME => array(
			'columns' => array('filterName', 'userEmail'),
			'primaryKey' => 'filterName',
			'indices' => array()
		),
		self::FILTERS_PROJECTS_TABLE_NAME => array(
			'columns' => array('filterName', 'pid'),
			'primaryKey' => 'filterName',
			'indices' => array()
		),
        self::DATE_DIMENSIONS_TABLE_NAME => array(
            'columns' => array('name', 'includeTime', 'isExported'),
            'primaryKey' => 'name',
            'indices' => array()
        ),
        self::DATA_SETS_TABLE_NAME => array(
            'columns' => array('id', 'name', 'export', 'isExported', 'lastChangeDate', 'incrementalLoad', 'ignoreFilter', 'definition'),
            'primaryKey' => 'id',
            'indices' => array()
        )
	);

	public static $columnDefinitions = array('gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
		'format', 'dateDimension', 'sortLabel', 'sortOrder');



	/**
	 * @var string
	 */
	public $writerId;


	/**
	 * Prepare configuration
	 * Get bucket attributes and backendUrl for Rest API calls
	 * @param $writerId
	 * @param StorageApiClient $storageApiClient
	 */
	public function __construct(StorageApiClient $storageApiClient, $writerId = null)
	{
		parent::__construct($storageApiClient);

		if ($writerId) {
			$this->writerId = $writerId;
			$this->bucketId = $this->configurationBucket($writerId);
			$this->tokenInfo = $this->_storageApiClient->getLogData();
			$this->projectId = $this->tokenInfo['owner']['id'];
		}

		//@TODO remove
		if ($this->bucketId && $this->sapi_bucketExists($this->bucketId) && !$this->sapi_tableExists($this->bucketId . '.' . self::DATA_SETS_TABLE_NAME)) {
			$this->migrateConfiguration();
		}
	}


	/********************
	 ********************
	 * @section Writer and it's bucket
	 ********************/


	/**
	 * Find configuration bucket for writerId
	 * @param $writerId
	 * @return bool
	 */
	public function configurationBucket($writerId)
	{
		foreach ($this->getWriters() as $w) {
			if ($w['writerId'] == $writerId) {
				return $w['bucket'];
			}
    	}
		return false;
	}


	/**
	 * @return array
	 */
	public function getWriters()
	{
		$writers = array();
		foreach ($this->sapi_listBuckets() as $bucket) {
			if (isset($bucket['attributes']) && is_array($bucket['attributes'])) {
				$bucketAttributes = $this->parseAttributes($bucket['attributes']);

				if (isset($bucketAttributes['writer']) && $bucketAttributes['writer'] == self::WRITER_NAME && isset($bucketAttributes['writerId'])) {
					$bucketAttributes['id'] = $bucketAttributes['writerId'];
					$bucketAttributes['bucket'] = $bucket['id'];
					$writers[] = $bucketAttributes;
				}
			}
		}
		return $writers;
	}


	/**
	 * Create configuration bucket for writer
	 * @param $writerId
	 * @param null $backendUrl
	 * @throws WrongParametersException
	 */
	public function createWriter($writerId, $backendUrl = null)
	{
		if ($this->configurationBucket($writerId)) {
			throw new WrongParametersException(sprintf("Writer with id '%s' already exists", $writerId));
		}

		$this->_storageApiClient->createBucket('wr-gooddata-' . $writerId, 'sys', 'GoodData Writer Configuration');
		$this->_storageApiClient->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'writer', self::WRITER_NAME);
		$this->_storageApiClient->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'writerId', $writerId);
		if ($backendUrl) {
			$this->_storageApiClient->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'gd.backendUrl', $backendUrl);
		}
		$this->bucketId = 'sys.c-wr-gooddata-' . $writerId;
	}


	/**
	 * Check if writer's bucket have all required attributes
	 * @throws WrongConfigurationException
	 */
	public function checkBucketAttributes()
	{
		$bucketAttributes = $this->bucketAttributes();
		$valid = !empty($bucketAttributes['gd']['pid'])
			&& !empty($bucketAttributes['gd']['username'])
			&& !empty($bucketAttributes['gd']['uid'])
			&& !empty($bucketAttributes['gd']['password']);

		if (!$valid) {
			throw new WrongConfigurationException('Writer is missing GoodData configuration');
		}
	}


	/**
	 * Update writer's configuration
	 * @param string $key
	 * @param string $value
	 * @param null $protected
	 */
	public function updateWriter($key, $value, $protected = null)
	{
		$this->_storageApiClient->setBucketAttribute($this->bucketId, $key, $value, $protected);
		$this->_cache['bucketInfo.' . $this->bucketId][$key] = $value; //@TODO
	}


	/**
	 * Delete writer configuration from SAPI
	 */
	public function deleteWriter()
	{
		foreach ($this->sapi_listTables($this->bucketId) as $table) {
			$this->_storageApiClient->dropTable($table['id']);
		}
		$this->_storageApiClient->dropBucket($this->bucketId);
	}



	/********************
	 ********************
	 * @section SAPI tables
	 ********************/


	/**
	 * Get output tables from SAPI
	 * @return array
	 */
	public function getOutputSapiTables()
	{
		$result = array();
		foreach ($this->sapi_listTables() as $table) {
			if (substr($table['id'], 0, 4) == 'out.') {
				$result[] = $table['id'];
			}
		}
		return $result;
	}


	/**
	 * Get info about table in SAPI
	 * @param $tableId
	 * @return mixed
	 * @throws WrongConfigurationException
	 */
	public function getSapiTable($tableId)
	{
		try {
			return $this->sapi_getTable($tableId);
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
		if (!empty($this->_cache[$cacheKey])) {
			return;
		}

		$tableId = $this->bucketId . '.' . self::DATA_SETS_TABLE_NAME;
		if (!$this->sapi_tableExists($tableId)) {
			$this->_createConfigTable(self::DATA_SETS_TABLE_NAME);
		}

		$outputTables = $this->getOutputSapiTables();
		$configuredTables = array();
		// Remove tables that does not exist from configuration
		$delete = array();
		foreach ($this->_fetchTableRows($tableId) as $row) {
			if (!in_array($row['id'], $outputTables)) {
				$delete[] = $row['id'];
			}
			if (!in_array($row['id'], $configuredTables)) {
				$configuredTables[] = $row['id'];
			}
		}
		if (count($delete)) {
			$this->_deleteTableRows($tableId, 'id', $delete);
		}

		// Add tables without configuration
		$add = array();
		foreach ($outputTables as $tableId) {
			if (!in_array($tableId, $configuredTables)) {
				$add[] = array('id' => $tableId);
			}
		}
		if (count($add)) {
			$this->_updateConfigTable(self::DATA_SETS_TABLE_NAME, $add);
		}

		if (count($delete) || count($add)) {
			$this->clearCache();
		}

		$this->_cache[$cacheKey] = true;
	}


	/**
	 * Get complete data set definition
	 * @param $tableId
	 * @return mixed
	 */
	public function getDataSetForApi($tableId)
	{
		$dataSet = $this->getDataSet($tableId);

		$previews = array();
		foreach($this->_fetchTableRows($tableId, null, null, array('limit' => 10)) as $row) {
			foreach ($row as $key => $value) {
				$previews[$key][] = $value;
			}
		}

		$columns = array();
		$sourceTable = $this->getSapiTable($tableId);
		foreach ($sourceTable['columns'] as $columnName) {
			$column = $dataSet['columns'][$columnName];
			$column['name'] = $columnName;
			if (!isset($column['gdName']))
				$column['gdName'] = $columnName;
			$column = $this->_cleanColumnDefinition($column);
			$column['preview'] = isset($previews[$columnName]) ? $previews[$columnName] : array();
			$columns[] = $column;
		}

		return array(
			'id' => $tableId,
			'name' => $dataSet['name'],
			'export' => (bool)$dataSet['export'],
			'isExported' => (bool)$dataSet['isExported'],
			'lastChangeDate' => $dataSet['lastChangeDate'] ? $dataSet['lastChangeDate'] : null,
			'incrementalLoad' => $dataSet['incrementalLoad'] ? (int)$dataSet['incrementalLoad'] : false,
			'ignoreFilter' => (bool)$dataSet['ignoreFilter'],
			'columns' => $columns,

			//@TODO deprecated
			'tableId' => $tableId,
			'lastExportDate' => $dataSet['isExported'] ? date('c') : null
		);
	}


	/**
	 * Get list of defined data sets
	 * @return array
	 */
	public function getDataSets()
	{
		$this->updateDataSetsFromSapi();
		$tables = array();
		foreach ($this->_getConfigTable(self::DATA_SETS_TABLE_NAME) as $table) {
			$tables[] = array(
				'id' => $table['id'],
				'bucket' => substr($table['id'], 0, strrpos($table['id'], '.')),
				'name' => $table['name'],
				'export' => (bool)$table['export'],
				'isExported' => (bool)$table['isExported'],
				'lastChangeDate' => $table['lastChangeDate'],
				'gdName' => $table['name'], //@TODO backwards compatibility with UI, remove soon!!
				'lastExportDate' => $table['isExported'] ? date('c') : null //@TODO backwards compatibility with UI, remove soon!!
			);
		}
		return $tables;
	}


	/**
	 * Get list of defined data sets with connection point
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 * @return array
	 * @TODO remove
	 */
	public function getDataSetsWithConnectionPointOld()
	{
		$this->updateDataSetsFromSapi();

		$tables = array();
		foreach ($this->_getConfigTable(self::DATA_SETS_TABLE_NAME) as $table) {
			$hasConnectionPoint = false;
			if (!empty($table['definition'])) {
				$tableDefinition = json_decode($table['definition'], true);
				if ($tableDefinition === NULL) {
					throw new WrongConfigurationException(sprintf("Definition of columns for table '%s' is not valid json", $table['id']));
				}
				foreach ($tableDefinition as $column) {
					if ($column['type'] == 'CONNECTION_POINT') {
						$hasConnectionPoint = true;
						break;
					}
				}
			}

			$tables[$table['id']] = array(
				'name' => $table['name'] ? $table['name'] : $table['id'],
				'referenceable' => $hasConnectionPoint
			);
		}
		return $tables;
	}

	public function getDataSetsWithConnectionPoint()
	{
		$this->updateDataSetsFromSapi();

		$tables = array();
		foreach ($this->_getConfigTable(self::DATA_SETS_TABLE_NAME) as $table) {
			$hasConnectionPoint = false;
			if (!empty($table['definition'])) {
				$tableDefinition = json_decode($table['definition'], true);
				if ($tableDefinition === NULL) {
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
	 * @param $tableId
	 * @return bool|mixed
	 * @throws WrongConfigurationException
	 */
	public function getDataSet($tableId)
	{
		$this->updateDataSetFromSapi($tableId);

		$tableConfig = $this->_getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
		if (!$tableConfig) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		if ($tableConfig['definition']) {
			$tableConfig['columns'] = json_decode($tableConfig['definition'], true);
			if ($tableConfig['columns'] === NULL) {
				throw new WrongConfigurationException("Definition of columns is not valid json");
			}
		} else {
			$tableConfig['columns'] = array();
		}

		return $tableConfig;
	}



	/**
	 * Check if data set has connection point
	 * @param $tableId
	 * @return array
	 */
	public function getDimensionsOfDataSet($tableId)
	{
		$dataSet = $this->getDataSet($tableId);

		$dimensions = array();
		foreach ($dataSet['columns'] as $column) {
			if ($column['type'] == 'DATE' && !empty($column['dateDimension'])) {
				$dimensions[] = $column['dateDimension'];
			}
		}
		return $dimensions;
	}


	/**
	 * Update definition of column of a data set
	 * @param $tableId
	 * @param $column
	 * @param $data
	 * @throws \Keboola\GoodDataWriter\Exception\WrongParametersException
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	public function updateColumnsDefinition($tableId, $column, $data = null)
	{
		$this->updateDataSetFromSapi($tableId);

		$tableRow = $this->_getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
		if (!$tableRow) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}
		if ($tableRow['definition']) {
			$definition = json_decode($tableRow['definition'], true);
			if ($definition === NULL) {
				throw new WrongConfigurationException("Definition of columns for table '$tableId' is not valid json");
			}
		} else {
			$definition = array();
		}

		if (is_array($column)) {
			// Update more columns
			foreach ($column as $columnData) {
				if (!isset($columnData['name'])) {
					throw new WrongParametersException("One of the columns is missing 'name' parameter");
				}
				$columnName = $columnData['name'];
				unset($columnData['name']);

				foreach (array_keys($columnData) as $key) if (!in_array($key, self::$columnDefinitions)) {
					throw new WrongParametersException(sprintf("Parameter '%s' is not valid for column definition", $key));
				}

				$definition[$columnName] = isset($definition[$columnName]) ? array_merge($definition[$columnName], $columnData) : $columnData;
				$definition[$columnName] = $this->_cleanColumnDefinition($definition[$columnName]);
			}
		} else {
			// Update one column
			if (!$data) {
				$data = array();
			}
			$definition[$column] = isset($definition[$column]) ? array_merge($definition[$column], $data) : $data;
			$definition[$column] = $this->_cleanColumnDefinition($definition[$column]);
		}

		$tableRow['definition'] = json_encode($definition);
		$tableRow['lastChangeDate'] = date('c');
		$this->_updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableRow);
	}


	/**
	 * Remove non-sense definitions
	 * @param $data
	 * @return mixed
	 */
	private function _cleanColumnDefinition($data)
	{
		if (empty($data['type'])) {
			$data['type'] = 'IGNORE';
		}
		if ($data['type'] != 'ATTRIBUTE' && isset($data['sortLabel'])) {
			unset($data['sortLabel']);
		}
		if ($data['type'] != 'ATTRIBUTE' && isset($data['sortOrder'])) {
			unset($data['sortOrder']);
		}
		if ($data['type'] != 'REFERENCE' && isset($data['schemaReference'])) {
			unset($data['schemaReference']);
		}
		if (!in_array($data['type'], array('HYPERLINK', 'LABEL')) && isset($data['reference'])) {
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
		} else {
			$data['dataTypeSize'] = (int)$data['dataTypeSize'];
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
	 * @param $tableId
	 * @param $name
	 * @param $value
	 * @throws \Keboola\GoodDataWriter\Exception\WrongParametersException
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	public function updateDataSetDefinition($tableId, $name, $value = null)
	{
		$this->updateDataSetFromSapi($tableId);

		$tableRow = $this->_getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
		if (!$tableRow) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		$allowedParams = $this->tables[Configuration::DATA_SETS_TABLE_NAME]['columns'];
		unset($allowedParams['id']);
		unset($allowedParams['lastChangeDate']);
		unset($allowedParams['definition']);

		if (is_array($name)) {
			unset($name['writerId']);
			// Update more values at once
			foreach (array_keys($name) as $key) if (!in_array($key, $allowedParams)) {
				throw new WrongParametersException(sprintf("Parameter '%s' is not valid for table definition", $key));
			}
			$tableRow = array_merge($tableRow, $name);
		} else {
			// Update one value
			if (!in_array($name, $allowedParams)) {
				throw new WrongParametersException(sprintf("Parameter '%s' is not valid for table definition", $name));
			}
			$tableRow[$name] = $value;
		}

		$tableRow['lastChangeDate'] = date('c');
		$this->_updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableRow);
	}


	/**
	 * Delete definition for columns removed from data table
	 * @param $tableId
	 * @throws WrongConfigurationException
	 */
	public function updateDataSetFromSapi($tableId)
	{
		// Do only once per request
		$cacheKey = 'updateDataSetFromSapi.' . $tableId;
		if (!empty($this->_cache[$cacheKey])) {
			return;
		}

		$anythingChanged = false;
		$table = $this->getSapiTable($tableId);
		$dataSet = $this->_getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
		if (!$dataSet) {
			$dataSet = array('id' => $tableId, 'definition' => array());
			$anythingChanged = true;
		}
		if ($dataSet['definition']) {
			$definition = json_decode($dataSet['definition'], true);
			if ($definition === NULL) {
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
			$definition = array();
		}

		// Added definitions for new columns
		foreach ($table['columns'] as $column) {
			if (!in_array($column, array_keys($definition))) {
				$definition[$column] = array('type' => 'IGNORE');
				$anythingChanged = true;
			}
		}

		if ($anythingChanged) {
			$dataSet['definition'] = json_encode($definition);
			$dataSet['lastChangeDate'] = date('c');
			$this->_updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $dataSet);
			$this->clearCache();
		}

		$this->_cache[$cacheKey] = true;
	}


	public function getXml($tableId)
	{
		$this->updateDataSetsFromSapi();
		$this->updateDataSetFromSapi($tableId);

		$gdDefinition = $this->getDataSet($tableId);
		$dateDimensions = null; // fetch only when needed
		$dataSetName = !empty($gdDefinition['name']) ? $gdDefinition['name'] : $gdDefinition['id'];
		$sourceTable = $this->getSapiTable($tableId);

		$xml = new \DOMDocument();
		$schema = $xml->createElement('schema');
		$name = $xml->createElement('name', $dataSetName);
		$schema->appendChild($name);
		$columns = $xml->createElement('columns');

		foreach ($sourceTable['columns'] as $columnName) {

			if (!isset($gdDefinition['columns'][$columnName])) {
				$columnDefinition = array(
					'name' => $columnName,
					'type' => 'IGNORE',
					'gdName' => $columnName,
					'dataType' => ''
				);
			} else {
				$columnDefinition = $gdDefinition['columns'][$columnName];
			}

			$column = $xml->createElement('column');
			$column->appendChild($xml->createElement('name', $columnName));
			$column->appendChild($xml->createElement('title', (!empty($columnDefinition['gdName']) ? $columnDefinition['gdName']
					: $columnName) . ' (' . $dataSetName . ')'));
			$column->appendChild($xml->createElement('ldmType', !empty($columnDefinition['type']) ? $columnDefinition['type'] : 'IGNORE'));
			if ($columnDefinition['type'] != 'FACT') {
				$column->appendChild($xml->createElement('folder', $dataSetName));
			}

			if (!empty($columnDefinition['dataType'])) {
				$dataType = $columnDefinition['dataType'];
				if (!empty($columnDefinition['dataTypeSize'])) {
					$dataType .= '(' . $columnDefinition['dataTypeSize'] . ')';
				}
				$column->appendChild($xml->createElement('dataType', $dataType));
			}

			if (!empty($columnDefinition['type'])) switch($columnDefinition['type']) {
				case 'ATTRIBUTE':
					if (!empty($columnDefinition['sortLabel'])) {
						$column->appendChild($xml->createElement('sortLabel', $columnDefinition['sortLabel']));
						$column->appendChild($xml->createElement('sortOrder', !empty($columnDefinition['sortOrder'])
							? $columnDefinition['sortOrder'] : 'ASC'));
					}
					break;
				case 'LABEL':
				case 'HYPERLINK':
					$column->appendChild($xml->createElement('reference', $columnDefinition['reference']));
					break;
				case 'DATE':
					if (!$dateDimensions) {
						$dateDimensions = $this->getDateDimensions();
					}
					if (!empty($columnDefinition['dateDimension']) && isset($dateDimensions[$columnDefinition['dateDimension']])) {
						$column->appendChild($xml->createElement('format', $columnDefinition['format']));
						$column->appendChild($xml->createElement('datetime',
							$dateDimensions[$columnDefinition['dateDimension']]['includeTime'] ? 'true' : 'false'));
						$column->appendChild($xml->createElement('schemaReference', $columnDefinition['dateDimension']));
					} else {
						throw new WrongConfigurationException("Date column '{$columnDefinition['name']}' does not have valid date dimension assigned");
					}
					break;
				case 'REFERENCE':
					if ($columnDefinition['schemaReference']) {
						try {
							$refTableDefinition = $this->getDataSet($columnDefinition['schemaReference']);
						} catch (WrongConfigurationException $e) {
							throw new WrongConfigurationException("Schema reference '{$columnDefinition['schemaReference']}'"
								. " of column '{$columnDefinition['name']}' does not exist");
						}
						if ($refTableDefinition) {
							$refTableName = !empty($refTableDefinition['name']) ? $refTableDefinition['name'] : $refTableDefinition['id'];
							$column->appendChild($xml->createElement('schemaReference', $refTableName));
							$reference = NULL;
							foreach ($refTableDefinition['columns'] as $cName => $c) {
								if ($c['type'] == 'CONNECTION_POINT') {
									$reference = $cName;
									break;
								}
							}
							if ($reference) {
								$column->appendChild($xml->createElement('reference', $reference));
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

			$columns->appendChild($column);
		}

		$schema->appendChild($columns);
		$xml->appendChild($schema);

		return $xml->saveXML();
	}



	/********************
	 ********************
	 * @section Date dimensions
	 ********************/


	/**
	 * Get defined date dimensions
	 * @param bool $usage
	 * @return array
	 */
	public function getDateDimensions($usage = false)
	{
		if ($usage) return $this->getDateDimensionsWithUsage();

		$tableId = $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME;
		if (!$this->sapi_tableExists($tableId)) {
			$this->_createConfigTable(self::DATE_DIMENSIONS_TABLE_NAME);
			return array();
		} else {
			$data = array();
			foreach ($this->_getConfigTable(self::DATE_DIMENSIONS_TABLE_NAME) as $row) {
				$row['includeTime'] = (bool)$row['includeTime'];
				$row['isExported'] = (bool)$row['isExported'];
				$data[$row['name']] = $row;
			}

			if (count($data)) {
				$this->_checkConfigTable(self::DATE_DIMENSIONS_TABLE_NAME, array_keys(current($data)));
			}
			return $data;
		}
	}


	/**
	 * Get defined date dimensions with usage in data sets
	 * @return array
	 */
	public function getDateDimensionsWithUsage()
	{
		$dimensions = $this->getDateDimensions();

		$usage = array();
		foreach ($this->getDataSets() as $dataSet) {
			foreach ($this->getDimensionsOfDataSet($dataSet['id']) as $dimension) {
				if (!isset($usage[$dimension])) {
					$usage[$dimension]['usedIn'] = array();
				}
				$usage[$dimension]['usedIn'][] = $dataSet['id'];
			}
		}

		return array_merge_recursive($dimensions, $usage);
	}


	/**
	 * Add date dimension
	 * @param $name
	 * @param $includeTime
	 */
	public function saveDateDimension($name, $includeTime)
	{
		$data = array(
			'name' => $name,
			'includeTime' => $includeTime,
			'isExported' => null
		);
		$this->_updateConfigTableRow(self::DATE_DIMENSIONS_TABLE_NAME, $data);
	}

	public function setDateDimensionIsExported($name)
	{
		$data = array(
			'name' => $name,
			'isExported' => 1
		);
		$this->_updateConfigTableRow(self::DATE_DIMENSIONS_TABLE_NAME, $data);
	}


	/**
	 * Delete date dimension
	 * @param $name
	 */
	public function deleteDateDimension($name)
	{
		$this->_deleteTableRow($this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME, 'name', $name);
	}




	/********************
	 ********************
	 * @section Project clones
	 ********************/


	/**
	 * Get list of all projects
	 * @return array
	 * @throws WrongConfigurationException
	 */
	public function getProjects()
	{
		$bucketAttributes = $this->bucketAttributes();
		$projects = $this->_getConfigTable(self::PROJECTS_TABLE_NAME);
		if (isset($bucketAttributes['gd']['pid'])) {
			array_unshift($projects, array('pid' => $bucketAttributes['gd']['pid'], 'active' => true, 'main' => true));
		}
		return $projects;
	}


	/**
	 * Get project if exists
	 * @param $pid
	 * @return bool|array
	 */
	public function getProject($pid)
	{
		foreach ($this->getProjects() as $project) {
			if ($project['pid'] == $pid) return $project;
		}
		return false;
	}


	/**
	 * Check configuration table of projects
	 * @throws WrongConfigurationException
	 */
	public function checkProjectsTable()
	{
		$tableId = $this->bucketId . '.' . self::PROJECTS_TABLE_NAME;
		if ($this->sapi_tableExists($tableId)) {
			$table = $this->getSapiTable($tableId);
			$this->_checkConfigTable(self::PROJECTS_TABLE_NAME, $table['columns']);
		}
	}


	/**
	 * @param $pid
	 */
	public function saveProject($pid)
	{
		$data = array(
			'pid' => $pid,
			'active' => 1
		);
		$this->_updateConfigTableRow(self::PROJECTS_TABLE_NAME, $data);
	}



	/********************
	 ********************
	 * @section Project users
	 ********************/


	/**
	 * Get list of all users
	 * @return array
	 * @throws WrongConfigurationException
	 */
	public function getUsers()
	{
		$bucketAttributes = $this->bucketAttributes();
		$users = $this->_getConfigTable(self::USERS_TABLE_NAME);
		if (isset($bucketAttributes['gd']['username']) && isset($bucketAttributes['gd']['uid'])) {
			array_unshift($users, array(
				'email' => $bucketAttributes['gd']['username'],
				'uid' => $bucketAttributes['gd']['uid'],
				'main' => true
			));
		}
		return $users;
	}


	/**
	 * Get user if exists
	 * @param $email
	 * @return bool|array
	 */
	public function getUser($email)
	{
		foreach ($this->getUsers() as $user) {
			if ($user['email'] == $email) return $user;
		}
		return false;
	}

	/**
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function checkUsersTable()
	{
		$tableId = $this->bucketId . '.' . self::USERS_TABLE_NAME;
		if ($this->sapi_tableExists($tableId)) {
			$table = $this->getSapiTable($tableId);
			$this->_checkConfigTable(self::USERS_TABLE_NAME, $table['columns']);
		}
	}

	/**
<<<<<<< HEAD
	 * Get users of specified project
=======
	 * Check if user was invited/added to project by writer
	 * @param $email
	 * @param $pid
	 * @return bool
	 */
	public function isProjectUser($email, $pid)
	{
		foreach ($this->getProjectUsers() AS $projectUser) {
			if ($projectUser['email'] == $email && $projectUser['pid'] == $pid && empty($projectUser['main']))
				return true;
		}

		return false;
	}

	/**
	 * @param null $pid
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function getProjectUsers($pid = null)
	{
		$bucketAttributes = $this->bucketAttributes();
		$projectUsers = $this->_getConfigTable(self::PROJECT_USERS_TABLE_NAME);
		if (!count($projectUsers) && isset($bucketAttributes['gd']['pid']) && isset($bucketAttributes['gd']['username'])) {
			array_unshift($projectUsers, array(
				'id' => 0,
				'pid' => $bucketAttributes['gd']['pid'],
				'email' => $bucketAttributes['gd']['username'],
				'role' => 'admin',
				'action' => 'add',
				'main' => true
			));
		}

		if ($pid) {
			$result = array();
			foreach ($projectUsers as $u) {
				if ($u['pid'] == $pid) {
					$result[] = array(
						'email' => $u['email'],
						'role' => $u['role']
					);
				}
			}
			return $result;
		}

		return $projectUsers;
	}

	/**
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function checkProjectUsersTable()
	{
		$tableId = $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME;
		if ($this->sapi_tableExists($tableId)) {
			$table = $this->getSapiTable($tableId);
			$this->_checkConfigTable(self::PROJECT_USERS_TABLE_NAME, $table['columns']);
		}
	}


	/**
	 * Save user to configuration
	 * @param $email
	 * @param $uid
	 */
	public function saveUser($email, $uid)
	{
		$data = array(
			'email' => $email,
			'uid' => $uid
		);
		$this->_updateConfigTableRow(self::USERS_TABLE_NAME, $data);
	}


	/**
	 * Save project user to configuration
	 * @param $pid
	 * @param $email
	 * @param $role
	 */
	public function saveProjectUser($pid, $email, $role)
	{
		$action = 'add';
		$data = array(
			'id' => sha1($pid . $email . $action . date('c')),
			'pid' => $pid,
			'email' => $email,
			'role' => $role,
			'action' => $action
		);
		$this->_updateConfigTableRow(self::PROJECT_USERS_TABLE_NAME, $data);
	}

	/**
	 * @param $pid
	 * @param $email
	 * @param $role
	 */
	public function saveProjectInvite($pid, $email, $role)
	{
		$action = 'invite';
		$data = array(
			'id' => sha1($pid . $email . $action . date('c')),
			'pid' => $pid,
			'email' => $email,
			'role' => $role,
			'action' => $action
		);
		$table = new StorageApiTable($this->_storageApiClient, $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, null, 'id');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		if (!$this->_storageApiClient->tableExists($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME)) {
			$table->addIndex('pid');
			$table->addIndex('email');
		}
		$table->save();
	}

	/**
	 * @param $pid
	 * @param $email
	 */
	public function removeProjectUserAdd($pid, $email)
	{
		$filter = array();
		foreach ($this->getProjectUsers() as $projectUser) {
			if (isset($projectUser['main']))
				continue;

			if ($projectUser['pid'] == $pid && $projectUser['email'] == $email && $projectUser['action'] == 'add')
				$filter[] = $projectUser['id'];
		}

		if (!$filter)
			return;

		$this->_storageApiClient->deleteTableRows(
			$this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME,
			array(
				'whereColumn' => 'id',
				'whereValues' => $filter,
			)
		);
	}

	/**
	 * @param $pid
	 * @param $email
	 */
	public function removeProjectUserInvite($pid, $email)
	{
		$filter = array();
		foreach ($this->getProjectUsers() as $projectUser) {
			if (isset($projectUser['main']))
				continue;

			if ($projectUser['pid'] == $pid && $projectUser['email'] == $email && $projectUser['action'] == 'invite')
				$filter[] = $projectUser['id'];
		}

		if (!$filter)
			return;

		$this->_storageApiClient->deleteTableRows(
			$this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME,
			array(
				'whereColumn' => 'id',
				'whereValues' => $filter,
			)
		);
	}




	/********************
	 ********************
	 * @section Filters
	 ********************/


	/**
	 * @param $name
	 * @return bool|array
	 */
	public function getFilter($name)
	{
		foreach ($this->getFilters() as $filter) {
			if ($filter['name'] == $name) {
				return $filter;
			}
		}
		return false;
	}

	/**
	 * @param $userEmail
	 * @param null $pid
	 * @return array
	 */
	public function getFiltersForUser($userEmail, $pid = null)
	{
		$filtersUsers = $this->getFiltersUsers();

		$filters = array();
		foreach ($filtersUsers as $fu) {
			if ($fu['userEmail'] == $userEmail) {
				$filter = $this->getFilter($fu['filterName']);
				if (null == $pid || strstr($filter['uri'], $pid)) {
					$filters[] = $filter;
				}
			}
		}

		return $filters;
	}


	/**
	 * @return array
	 */
	public function getFilters()
	{
		return $this->_getConfigTable(self::FILTERS_TABLE_NAME);
	}


	/**
	 * @return array
	 */
	public function getFiltersUsers()
	{
		return $this->_getConfigTable(self::FILTERS_USERS_TABLE_NAME);
	}

	/**
	 * @return array
	 */
	public function getFiltersProjects()
	{
		return $this->_getConfigTable(self::FILTERS_PROJECTS_TABLE_NAME);
	}

	/**
	 *
	 * @param string $name
	 * @param string $attribute
	 * @param string $element
	 * @param string $operator
	 * @param string $uri
	 * @throws \Keboola\GoodDataWriter\Exception\WrongParametersException
	 */
	public function saveFilter($name, $attribute, $element, $operator, $uri)
	{
		// check for existing name
		foreach ($this->getFilters() as $f) {
			if ($f['name'] == $name) {
				throw new WrongParametersException("Filter of that name already exists.");
			}
		}

		$data = array(
			'name' => $name,
			'attribute' => $attribute,
			'element' => $element,
			'operator' => $operator,
			'uri' => $uri
		);
		$this->_updateConfigTableRow(self::FILTERS_TABLE_NAME, $data);
	}

	public function saveFiltersProjects($filterName, $pid)
	{
		foreach($this->getFiltersProjects() as $fp) {
			if ($fp['filterName'] == $filterName && $fp['pid'] == $pid) {
				throw new WrongParametersException("Filter " . $filterName . " is already assigned to project " . $pid);
			}
		}

		$data = array(
			'filterName' => $filterName,
			'pid' => $pid
		);
		$this->_updateConfigTableRow(self::FILTERS_PROJECTS_TABLE_NAME, $data);
	}

	/**
	 * Update URI of the filter
	 *
	 * @param $name
	 * @param $attribute
	 * @param $element
	 * @param $operator
	 * @param $uri
	 */
	public function updateFilters($name, $attribute, $element, $operator, $uri)
	{
		$data = $this->getFilters();

		foreach ($data as $k => $v) {
			if ($v['name'] == $name) {
				$data[$k] = array($name, $attribute, $element, $operator, $uri);
				break;
			}
		}

		$this->_updateConfigTable(self::FILTERS_TABLE_NAME, $data, false);
	}

	/**
	 * @param array $filters
	 * @param $userEmail
	 */
	public function saveFilterUser(array $filters, $userEmail)
	{
		$filterNames = array();
		foreach ($filters as $filterUri) {
			foreach ($this->getFilters() as $filter) {
				if ($filter['uri'] == $filterUri) {
					$filterNames[] = $filter['name'];
				}
			}
		}

		$data = array();
		foreach ($filterNames as $fn) {
			$data[] = array($fn, $userEmail);
		}

		$this->_updateConfigTable(self::FILTERS_USERS_TABLE_NAME, $data, false);
	}

	public function deleteFilter($filterUri)
	{
		$filters = array();

		$filterName = null;
		foreach ($this->getFilters() as $filter) {
			if ($filter['uri'] != $filterUri) {
				$filters[] = $filter;
			} else {
				$filterName = $filter['name'];
			}
		}

		if (empty($filters)) {
			$this->_storageApiClient->dropTable($this->bucketId . '.' . self::FILTERS_TABLE_NAME);
		} else {
			$this->_updateConfigTable(self::FILTERS_TABLE_NAME, $filters);
		}

		// Update filtersUsers table
		$filtersUsers = array();
		foreach ($this->getFiltersUsers() as $row) {
			if ($row['filterName'] != $filterName) {
				$filtersUsers[] = $row;
			}
		}

		if (empty($filtersUsers)) {
			$this->_storageApiClient->dropTable($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME);
		} else {
			$this->_updateConfigTable(self::FILTERS_USERS_TABLE_NAME, $filtersUsers);
		}

		// Update filtersProjects table
		$filtersProjects = array();
		foreach ($this->getFiltersProjects() as $row) {
			if ($row['filterName'] != $filterName) {
				$filtersProjects[] = $row;
			}
		}

		if (empty($filtersProjects)) {
			$this->_storageApiClient->dropTable($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME);
		} else {
			$this->_updateConfigTable(self::FILTERS_PROJECTS_TABLE_NAME, $filtersProjects);
		}
	}

	/**
	 * @TODO should be moved to RestApi class
	 * Translates attribute name from SAPI form to GD form
	 * Example: out.c-main.users.id -> attr.outcmainusers.id
	 * If name is set on SAPI table (name = users): out.c-main.users.id -> attr.users.id
	 * @param $attribute
	 * @return string
	 */
	public function translateAttributeName($attribute)
	{
		$idArr = explode('.', $attribute);
		$tableId = $idArr[0] . '.' . $idArr[1] . '.' . $idArr[2];
		$attrName = $idArr[3];

		$tableDef = $this->getDataSet($tableId);

		$tableName = $tableId;
		if (!empty($tableDef['name'])) {
			$tableName = $tableDef['name'];
		}

		return strtolower('attr.' . preg_replace('/[^a-z\d ]/i', '', $tableName) . '.' . $attrName);
	}




	/**
	 * Migrate from old configuration if applicable
	 */
	public function migrateConfiguration()
	{
		$this->_createConfigTable(self::DATA_SETS_TABLE_NAME);

		foreach ($this->sapi_listTables($this->bucketId) as $table) {
			if ($table['name'] == 'dateDimensions') {
				if (!$this->sapi_tableExists($this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME)) {
					$this->_createConfigTable(self::DATE_DIMENSIONS_TABLE_NAME);
					$data = array();
					foreach ($this->_fetchTableRows($table['id']) as $row) {
						$data[] = array('name' => $row['name'], 'includeTime' => $row['includeTime']);
					}
					$this->_updateConfigTable(self::DATE_DIMENSIONS_TABLE_NAME, $data, false);
					//@TODO $this->_storageApiClient->dropTable($table['id']);
				}
			}
			if (!in_array($table['name'], array_keys($this->tables)) && $table['name'] != 'dateDimensions') {
				$configTable = $this->getSapiTable($table['id']);
				$dataSetRow = array(
					'id' => null,
					'name' => null,
					'export' => null,
					'isExported' => null,
					'lastChangeDate' => null,
					'incrementalLoad' => null,
					'ignoreFilter' => null,
					'definition' => null
				);
				if (count($configTable['attributes'])) foreach ($configTable['attributes'] as $attribute) {
					if ($attribute['name'] == 'tableId') {
						$dataSetRow['id'] = $attribute['value'];
					}
					if ($attribute['name'] == 'gdName') {
						$dataSetRow['name'] = $attribute['value'];
					}
					if ($attribute['name'] == 'export') {
						$dataSetRow['export'] = empty($attribute['value']) ? 0 : 1;
					}
					if ($attribute['name'] == 'lastExportDate') {
						$dataSetRow['isExported'] = empty($attribute['value']) ? 0 : 1;
					}
					if ($attribute['name'] == 'lastChangeDate') {
						$dataSetRow['lastChangeDate'] = $attribute['value'];
					}
					if ($attribute['name'] == 'incrementalLoad') {
						$dataSetRow['incrementalLoad'] = (int)$attribute['value'];
					}
					if ($attribute['name'] == 'ignoreFilter') {
						$dataSetRow['ignoreFilter'] = empty($attribute['value']) ? 0 : 1;
					}
				}
				$columns = array();
				foreach ($this->_fetchTableRows($table['id']) as $colDef) {
					$colName = $colDef['name'];
					unset($colDef['name']);
					$columns[$colName] = $this->_cleanColumnDefinition($colDef);
				}
				$dataSetRow['definition'] = json_encode($columns);
				$this->_updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $dataSetRow);
				//@TODO $this->_storageApiClient->dropTable($table['id']);
			}
		}
	}

}
