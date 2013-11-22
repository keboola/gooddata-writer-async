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
	Keboola\StorageApi\Config\Reader,
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
	protected static $_tablesConfiguration = array(
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
            'columns' => array('name', 'includeTime'),
            'primaryKey' => 'name',
            'indices' => array()
        ),
        self::DATA_SETS_TABLE_NAME => array(
            'columns' => array('id', 'name', 'export', 'isExported', 'lastChangeDate', 'incrementalLoad', 'ignoreFilter', 'definition'),
            'primaryKey' => 'id',
            'indices' => array()
        )
	);



	/**
	 * @var array|string
	 */
	public $bucketInfo;
	/**
	 * @var array
	 */
	public $tokenInfo;
	/**
	 * @var int
	 */
	public $projectId;
	/**
	 * @var string
	 */
	public $writerId;
	/**
	 * @var string
	 */
	public $backendUrl;


	/**
	 * Prepare configuration
	 * Get bucket attributes and backendUrl for Rest API calls
	 * @param $writerId
	 * @param StorageApiClient $storageApiClient
	 */
	public function __construct($writerId, StorageApiClient $storageApiClient)
	{
		$this->writerId = $writerId;
		$this->_storageApiClient = $storageApiClient;

		$this->bucketId = $this->configurationBucket($writerId);
		$this->tokenInfo = $this->_storageApiClient->verifyToken();
		$this->projectId = $this->tokenInfo['owner']['id'];

		if ($this->bucketId && $this->_storageApiClient->bucketExists($this->bucketId)) {
			Reader::$client = $this->_storageApiClient;
			$this->bucketInfo = Reader::read($this->bucketId, null, false);
			$this->backendUrl = !empty($this->bucketInfo['gd']['backendUrl']) ? $this->bucketInfo['gd']['backendUrl'] : null;
		}

		self::$_tables = self::$_tablesConfiguration;
		self::$_sapiCache = array();

		//@TODO remove
		if ($this->bucketId && $this->_storageApiClient->bucketExists($this->bucketId)) {
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
		foreach (self::getWriters($this->_storageApiClient) as $w) {
			if ($w['id'] == $writerId) {
				return $w['bucket'];
			}
    	}
		return false;
	}


	/**
	 * @param StorageApiClient $storageApi
	 * @return array
	 */
	public static function getWriters($storageApi)
	{
		$writers = array();
		foreach (self::sapi_listBuckets($storageApi) as $bucket) {
			$writerId = false;
			$foundWriterType = false;
			if (isset($bucket['attributes']) && is_array($bucket['attributes'])) foreach($bucket['attributes'] as $attribute) {
				if ($attribute['name'] == 'writerId') {
					$writerId = $attribute['value'];
				}
				if ($attribute['name'] == 'writer') {
					$foundWriterType = $attribute['value'] == self::WRITER_NAME;
				}
				if ($writerId && $foundWriterType) {
					break;
				}
			}
			if ($writerId && $foundWriterType) {
				$writers[] = array(
					'id' => $writerId,
					'bucket' => $bucket['id']
				);
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
		$valid = !empty($this->bucketInfo['gd']['pid'])
			&& !empty($this->bucketInfo['gd']['username'])
			&& !empty($this->bucketInfo['gd']['uid'])
			&& !empty($this->bucketInfo['gd']['password']);

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
		$this->bucketInfo[$key] = $value;
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
		foreach (self::sapi_listBuckets($this->_storageApiClient) as $bucket) {
			if (substr($bucket['id'], 0, 3) == 'out') {
				foreach ($this->sapi_listTables($bucket['id']) as $table) {
					$result[] = $table['id'];
				}
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
		$tableId = $this->bucketId . '.' . self::DATA_SETS_TABLE_NAME;
		if (!$this->sapi_tableExists($tableId)) {
			$this->_createConfigTable(self::DATA_SETS_TABLE_NAME);
		}

		$outputTables = $this->getOutputSapiTables();
		$configuredTables = array();
		// Remove tables that does not exist from configuration
		foreach ($this->_fetchTableRows($tableId) as $row) {
			if (!in_array($row['id'], $outputTables)) {
				$this->_deleteTableRow($tableId, 'id', $row['id']);
			}
			if (!in_array($row['id'], $configuredTables)) {
				$configuredTables[] = $row['id'];
			}
		}

		// Add tables without configuration
		foreach ($outputTables as $tableId) {
			if (!in_array($tableId, $configuredTables)) {
				$this->_updateConfigTableRow(self::DATA_SETS_TABLE_NAME, array(
					'id' => $tableId
				));
			}
		}
	}


	/**
	 * Get complete data set definition
	 * @param $tableId
	 * @return mixed
	 */
	public function getDataSetForApi($tableId)
	{
		$this->updateDataSetsFromSapi();

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
			$column['preview'] = isset($previews[$columnName]) ? $previews[$columnName] : array();
			$columns[] = $this->_cleanColumnDefinition($column);
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
	 * @return array
	 */
	public function getDataSetsWithConnectionPoint()
	{
		$this->updateDataSetsFromSapi();

		$tables = array();
		foreach ($this->_getConfigTable(self::DATA_SETS_TABLE_NAME) as $table) {
			$tables[$table['id']] = array(
				'name' => $table['name'] ? $table['name'] : $table['id'],
				'referenceable' => $this->dataSetHasConnectionPoint($table['id'])
			);
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
			if (!$tableConfig['columns']) {
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
	 * @return bool
	 */
	public function dataSetHasConnectionPoint($tableId)
	{
		$tableConfig = $this->getDataSet($tableId);

		foreach ($tableConfig['columns'] as $column) {
			if ($column['type'] == 'CONNECTION_POINT') {
				return true;
			}
		}
		return false;
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
	 * @throws WrongConfigurationException
	 */
	public function updateColumnDefinition($tableId, $column, $data)
	{
		$this->updateDataSetsFromSapi();
		$this->updateDataSetFromSapi($tableId);

		$tableRow = $this->_getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
		if (!$tableRow) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		if ($tableRow['definition']) {
			$definition = json_decode($tableRow['definition'], true);
			if (!$definition) {
				throw new WrongConfigurationException("Definition of columns for table '$tableId' is not valid json");
			}
			$definition[$column] = isset($definition[$column]) ? array_merge($definition[$column], $data) : $data;
			$definition[$column] = $this->_cleanColumnDefinition($definition[$column]);

			$tableRow['definition'] = json_encode($definition);

		} else {
			$tableRow['definition'] = json_encode(array($column => $data));
		}

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
		if (!in_array($data['type'], array('REFERENCE', 'HYPERLINK', 'LABEL')) && isset($data['reference'])) {
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
		return $data;
	}


	/**
	 * Update definition of data set
	 * @param $tableId
	 * @param $name
	 * @param $value
	 * @throws WrongConfigurationException
	 */
	public function updateDataSetDefinition($tableId, $name, $value)
	{
		$this->updateDataSetsFromSapi();
		$this->updateDataSetFromSapi($tableId);

		$tableRow = $this->_getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
		if (!$tableRow) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}
		if (!isset($tableRow[$name])) {
			throw new WrongConfigurationException("DataSet does not have '$name' definition parameter");
		}

		$tableRow[$name] = $value;
		$this->_updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableRow);
	}


	/**
	 * Delete definition for columns removed from data table
	 * @param $tableId
	 * @throws WrongConfigurationException
	 */
	public function updateDataSetFromSapi($tableId)
	{
		if (!$this->sapi_tableExists($tableId)) {
			throw new WrongConfigurationException("Table '$tableId' does not exist");
		}

		$anythingChanged = false;
		$table = $this->getSapiTable($tableId);
		$dataSet = $this->_getConfigTableRow(self::DATA_SETS_TABLE_NAME, $tableId);
		if (!$dataSet) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}
		if ($dataSet['definition']) {
			$definition = json_decode($dataSet['definition'], true);
			if (!$definition) {
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
			$this->_updateConfigTableRow(self::DATA_SETS_TABLE_NAME, $dataSet);

			$this->updateDataSetDefinition($tableId, 'lastChangeDate', date('c'));
		}
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
				if ($columnDefinition['dataTypeSize']) {
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
									$reference = !empty($c['gdName']) ? $c['gdName'] : $cName;
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
				$data[$row['name']] = $row;
			}

			if (count($data)) {
				self::_checkConfigTable(self::DATE_DIMENSIONS_TABLE_NAME, array_keys(current($data)));
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
			'includeTime' => $includeTime
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
		$projects = self::getConfigTable(self::PROJECTS_TABLE_NAME);
		if (isset($this->bucketInfo['gd']['pid'])) {
			array_unshift($projects, array('pid' => $this->bucketInfo['gd']['pid'], 'active' => true, 'main' => true));
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
			self::_checkConfigTable(self::PROJECTS_TABLE_NAME, $table['columns']);
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
		$users = self::getConfigTable(self::USERS_TABLE_NAME);
		if (isset($this->bucketInfo['gd']['username']) && isset($this->bucketInfo['gd']['uid'])) {
			array_unshift($users, array(
				'email' => $this->bucketInfo['gd']['username'],
				'uid' => $this->bucketInfo['gd']['uid'],
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
			self::_checkConfigTable(self::USERS_TABLE_NAME, $table['columns']);
		}
	}

	/**
	 * Get users of specified project
	 * @param null $pid
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function getProjectUsers($pid = null)
	{
		$projectUsers = self::getConfigTable(self::PROJECT_USERS_TABLE_NAME);
		if (!count($projectUsers) && isset($this->bucketInfo['gd']['pid']) && isset($this->bucketInfo['gd']['username'])) {
			array_unshift($projectUsers, array(
				'id' => 0,
				'pid' => $this->bucketInfo['gd']['pid'],
				'email' => $this->bucketInfo['gd']['username'],
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
			self::_checkConfigTable(self::PROJECT_USERS_TABLE_NAME, $table['columns']);
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
		return self::getConfigTable(self::FILTERS_TABLE_NAME);
	}


	/**
	 * @return array
	 */
	public function getFiltersUsers()
	{
		return self::getConfigTable(self::FILTERS_USERS_TABLE_NAME);
	}

	/**
	 * @return array
	 */
	public function getFiltersProjects()
	{
		return self::getConfigTable(self::FILTERS_PROJECTS_TABLE_NAME);
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
		if (isset($tableDef['name'])) {
			$tableName = $tableDef['name'];
		}

		return strtolower('attr.' . preg_replace('/[^a-z\d ]/i', '', $tableName) . '.' . $attrName);
	}


	/**
	 * @param $tableName
	 * @param $columns
	 * @return bool
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	protected static function _checkConfigTable($tableName, $columns)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		if ($columns != self::$_tables[$tableName]['columns']) {
			throw new WrongConfigurationException(sprintf("Table '%s' appears to be wrongly configured. Contains columns: '%s' but should contain columns: '%s'",
				$tableName, implode(',', $columns), implode(',', self::$_tables[$tableName]['columns'])));
		}

		return true;
	}


	/**
	 * Migrate from old configuration if applicable
	 */
	public function migrateConfiguration()
	{
		if (!$this->sapi_tableExists($this->bucketId . '.' . self::DATA_SETS_TABLE_NAME)) {
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
				if (!in_array($table['name'], array_keys(self::$_tables)) && $table['name'] != 'dateDimensions') {
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

}
