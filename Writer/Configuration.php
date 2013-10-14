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
	Keboola\StorageApi\Exception as StorageApiException,
	Keboola\StorageApi\Config\Reader,
	Keboola\Csv\CsvFile,
	Keboola\Csv\Exception as CsvFileException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class Configuration extends StorageApiConfiguration
{
	const WRITER_NAME = 'gooddata';

	const PROJECTS_TABLE_NAME = 'projects';
	const USERS_TABLE_NAME = 'users';
	const PROJECT_USERS_TABLE_NAME = 'project_users';
	const FILTERS_TABLE_NAME = 'filters';
	const FILTERS_USERS_TABLE_NAME = 'filters_users';
	const FILTERS_PROJECTS_TABLE_NAME = 'filters_projects';
	const DATE_DIMENSIONS_TABLE_NAME = 'dateDimensions';

	/**
	 * Definition serves for automatic configuration of Storage API tables
	 * @var array
	 */
	protected static $_tables = array(
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
			'columns' => array('name', 'includeTime', 'lastExportDate'),
			'primaryKey' => 'name',
			'indices' => array()
		)
	);

	protected static $_cache = array(
		self::PROJECTS_TABLE_NAME => array(),
		self::USERS_TABLE_NAME => array(),
		self::PROJECT_USERS_TABLE_NAME => array(),
		self::FILTERS_TABLE_NAME => array(),
		self::FILTERS_USERS_TABLE_NAME => array(),
		self::FILTERS_PROJECTS_TABLE_NAME => array(),
		self::DATE_DIMENSIONS_TABLE_NAME => array(
			'dimensions' => array(),
			'usage' => array()
		)
	);


	/**
	 * @var string
	 */
	public $bucketId;
	/**
	 * @var array|string
	 */
	public $bucketInfo;
	/**
	 * @var array
	 */
	public $definedTables;
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


	public function __construct($writerId, StorageApiClient $storageApiClient)
	{
		$this->writerId = $writerId;
		$this->_storageApiClient = $storageApiClient;

		$this->bucketId = $this->configurationBucket($writerId);
		$this->tokenInfo = $this->_storageApiClient->verifyToken();
		$this->projectId = $this->tokenInfo['owner']['id'];

		$this->definedTables = array();
		if ($this->bucketId && $this->_storageApiClient->bucketExists($this->bucketId)) {
			Reader::$client = $this->_storageApiClient;
			$this->bucketInfo = Reader::read($this->bucketId, null, false);

			if (isset($this->bucketInfo['items'])) {
				foreach ($this->bucketInfo['items'] as $tableName => $table ) if (isset($table['tableId'])) {
					$this->definedTables[$table['tableId']] = array_merge($table, array('definitionId' => $this->bucketId . '.' . $tableName));
				}
				unset($this->bucketInfo['items']);
			}

			$this->backendUrl = !empty($this->bucketInfo['gd']['backendUrl']) ? $this->bucketInfo['gd']['backendUrl'] : null;
		}
	}



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
		foreach ($storageApi->listBuckets() as $bucket) {
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


	/*
	 * @TODO remove userUri check
	 */
	public function checkGoodDataSetup()
	{
		$valid = !empty($this->bucketInfo['gd']['pid'])
			&& !empty($this->bucketInfo['gd']['username'])
			&& (!empty($this->bucketInfo['gd']['userUri']) || !empty($this->bucketInfo['gd']['uid']))
			&& !empty($this->bucketInfo['gd']['password']);

		if (empty($this->bucketInfo['gd']['uid']) && !empty($this->bucketInfo['gd']['userUri'])) {
			if (substr($this->bucketInfo['gd']['userUri'], 0, 21) == '/gdc/account/profile/') {
				$this->bucketInfo['gd']['uid'] = substr($this->bucketInfo['gd']['userUri'], 21);
				$this->_storageApiClient->setBucketAttribute($this->bucketId, 'gd.uid', $this->bucketInfo['gd']['uid']);
				$this->_storageApiClient->deleteBucketAttribute($this->bucketId, 'gd.userUri');
				unset($this->bucketInfo['gd']['userUri']);
			} else {
				$valid = false;
			}
		}

		if (!$valid) {
			throw new WrongConfigurationException('Writer is missing GoodData configuration');
		}
	}


	public function getOutputTables()
	{
		if (empty(self::$_cache['outputTablesList'])) {
			self::$_cache['outputTablesList'] = array();
			foreach ($this->_storageApiClient->listBuckets() as $bucket) {
				if (substr($bucket['id'], 0, 3) == 'out') {
					foreach ($this->_storageApiClient->listTables($bucket['id']) as $table) {
						self::$_cache['outputTablesList'][] = $table['id'];
					}
				}
			}
		}
		return self::$_cache['outputTablesList'];
	}

	public function getTable($tableId)
	{
		if (!isset(self::$_cache['getTable'])) self::$_cache['getTable'] = array();

		if (!isset(self::$_cache['getTable'][$tableId])) {
			if (!$this->_storageApiClient->tableExists($tableId)) {
				throw new WrongConfigurationException("Table '$tableId' does not exist");
			}

			self::$_cache['getTable'][$tableId] = $this->_storageApiClient->getTable($tableId);
		}

		return self::$_cache['getTable'][$tableId];
	}

	public function getTableForApi($tableId)
	{
		$tableDefinition = array();

		foreach ($this->_fetchTableRows($this->definedTables[$tableId]['definitionId']) as $row) {

			if ($row['type'] != 'ATTRIBUTE' && $row['sortLabel']) {
				$row['sortLabel'] = null;
			}
			if ($row['type'] != 'ATTRIBUTE' && $row['sortOrder']) {
				$row['sortOrder'] = null;
			}
			if ($row['type'] != 'REFERENCE' && $row['schemaReference']) {
				$row['schemaReference'] = null;
			}
			if (!in_array($row['type'], array('REFERENCE', 'HYPERLINK', 'LABEL')) && $row['reference']) {
				$row['reference'] = null;
			}
			if ($row['type'] != 'DATE' && $row['format']) {
				$row['format'] = null;
			}
			if ($row['type'] != 'DATE' && $row['dateDimension']) {
				$row['dateDimension'] = null;
			}
			if (!empty($row['dataTypeSize'])) {
				$row['dataTypeSize'] = (int)$row['dataTypeSize'];
			}

			$tableDefinition[$row['name']] = $row;
		}

		$sourceTableInfo = $this->getTable($tableId);
		$this->checkMissingColumns($tableId, array('columns' => $tableDefinition), $sourceTableInfo['columns']);


		$data = $this->definedTables[$tableId];
		$data['tableId'] = $tableId;
		unset($data['definitionId']);
		$data['columns'] = array();

		$previews = array();
		if ($this->_storageApiClient->tableExists($tableId)) {
			foreach($this->_fetchTableRows($tableId, null, null, array('limit' => 10)) as $row) {
				foreach ($row as $key => $value) {
					$previews[$key][] = $value;
				}
			}
		}

		foreach ($sourceTableInfo['columns'] as $columnName) {
			$column = isset($tableDefinition[$columnName]) ? $tableDefinition[$columnName]
				: array('name' => $columnName, 'gdName' => $columnName, 'type' => 'IGNORE');
			$column['preview'] = isset($previews[$columnName]) ? $previews[$columnName] : array();
			if (!$column['gdName'])
				$column['gdName'] = $column['name'];
			$data['columns'][] = $column;
		}

		return $data;
	}

	public function getTables()
	{
		$tables = array();
		foreach ($this->_storageApiClient->listTables() as $table) {
			if (substr($table['id'], 0, 4) == 'out.') {
				$t = array(
					'id' => $table['id'],
					'bucket' => $table['bucket']['id']
				);
				if (isset($this->definedTables[$table['id']])) {
					$tableDef = $this->definedTables[$table['id']];
					$t['gdName'] = isset($tableDef['gdName']) ? $tableDef['gdName'] : null;
					$t['export'] = isset($tableDef['export']) ? (Boolean)$tableDef['export'] : false;
					$t['lastChangeDate'] = isset($tableDef['lastChangeDate']) ? $tableDef['lastChangeDate'] : null;
					$t['lastExportDate'] = isset($tableDef['lastExportDate']) ? $tableDef['lastExportDate'] : null;
				}
				$tables[] = $t;
			}
		}
		return $tables;
	}

	public function getReferenceableTables()
	{
		$tables = array();
		foreach ($this->definedTables as $table) {
			$tables[$table['tableId']] = array(
				'name' => isset($table['gdName']) ? $table['gdName'] : $table['tableId'],
				'referenceable' => $this->tableIsReferenceable($table['tableId'])
			);
		}
		return $tables;
	}

	public function getTableDefinition($tableId)
	{
		if (!isset(self::$_cache['tableDefinition'])) self::$_cache['tableDefinition'] = array();

		if (!isset(self::$_cache['tableDefinition'][$tableId])) {
			if (!isset($this->definedTables[$tableId])) {
				throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
			}

			$data = array('columns' => array());

			$tableInfo = $this->getTable($this->definedTables[$tableId]['definitionId']);
			if (isset($tableInfo['attributes'])) foreach ($tableInfo['attributes'] as $attr) {
				$data[$attr['name']] = $attr['value'];
			}

			foreach ($this->_fetchTableRows($this->definedTables[$tableId]['definitionId']) as $row) {
				$data['columns'][$row['name']] = $row;
			}
			self::$_cache['tableDefinition'][$tableId] = $data;
		}

		return self::$_cache['tableDefinition'][$tableId];
	}

	public function tableIsReferenceable($tableId)
	{
		foreach ($this->_fetchTableRows($this->definedTables[$tableId]['definitionId']) as $row) {
			if ($row['type'] == 'CONNECTION_POINT') {
				return true;
			}
		}
		return false;
	}

	public function createTableDefinition($tableId)
	{
		if (!isset(self::$_cache['tableDefinition'][$tableId])) {
			if (!isset($this->definedTables[$tableId])) {

				$tId = mb_substr($tableId, mb_strlen(StorageApiClient::STAGE_OUT) + 1);
				$bucket = mb_substr($tId, 0, mb_strpos($tId, '.'));
				$tableName = mb_substr($tId, mb_strpos($tId, '.')+1);
				$tableDefinitionId = $this->bucketId . '.' . $bucket . '_' . $tableName;

				$this->_createTable($tableDefinitionId, 'name', array('name', 'gdName', 'type', 'dataType',
					'dataTypeSize', 'schemaReference', 'reference', 'format', 'dateDimension', 'sortLabel', 'sortOrder'));
				$this->_storageApiClient->setTableAttribute($tableDefinitionId, 'tableId', $tableId);
				$this->_storageApiClient->setTableAttribute($tableDefinitionId, 'lastChangeDate', null);
				$this->_storageApiClient->setTableAttribute($tableDefinitionId, 'lastExportDate', null);


				$this->definedTables[$tableId] = array(
					'tableId' => $tableId,
					'gdName' => null,
					'lastChangeDate' => null,
					'lastExportDate' => null,
					'definitionId' => $tableDefinitionId
				);

			}
		}
	}

	public function saveColumnDefinition($tableId, $data)
	{
		if (!isset($this->definedTables[$tableId])) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		$this->_updateTableRow($this->definedTables[$tableId]['definitionId'], 'name', $data);
	}

	public function setTableAttribute($tableId, $name, $value)
	{
		if (!isset($this->definedTables[$tableId])) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		$this->definedTables[$tableId][$name] = $value;
		$this->_storageApiClient->setTableAttribute($this->definedTables[$tableId]['definitionId'], $name, $value);
	}

	public function getDateDimensions($usage = false)
	{
		if ($usage) return $this->getDateDimensionsWithUsage();

		if (!count(self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'])) {
			$tableId = $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME;
			if ($this->_storageApiClient->tableExists($tableId)) {
				$data = array();
				foreach ($this->_fetchTableRows($tableId) as $row) {
					$row['includeTime'] = (bool)$row['includeTime'];
					$data[$row['name']] = $row;
				}
				self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'] = $data;

				if (count(self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'])) {
					self::_checkConfigTable(self::DATE_DIMENSIONS_TABLE_NAME,
						array_keys(current(self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'])));
				}
			} else {
				$this->_createConfigTable(self::DATE_DIMENSIONS_TABLE_NAME);
			}
		}
		return self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'];
	}

	public function getDateDimensionsWithUsage()
	{
		$dimensions = $this->getDateDimensions();

		if (!count(self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['usage'])) {
			$usage = array();
			foreach (array_keys($this->definedTables) as $tId) {
				foreach ($this->tableDateDimensions($tId) as $dim) {
					if (!isset($usage[$dim])) {
						$usage[$dim]['usedIn'] = array();
					}
					$usage[$dim]['usedIn'][] = $tId;
				}
			}
			self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['usage'] = $usage;
		}

		return array_merge_recursive($dimensions, self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['usage']);
	}

	public function addDateDimension($name, $includeTime)
	{
		$data = array(
			'name' => $name,
			'includeTime' => $includeTime,
			'lastExportDate' => ''
		);
		$this->_updateConfigTableRow(self::DATE_DIMENSIONS_TABLE_NAME, $data);
		if (!self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'])
			$this->getDateDimensions();
		self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'][$name] = $data;
	}

	public function deleteDateDimension($name)
	{
		$this->_storageApiClient->deleteTableRows($this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME, array(
			'whereColumn' => 'name',
			'whereValues' => array($name)
		));
		if (!self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'])
			$this->getDateDimensions();
		unset(self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'][$name]);
	}

	public function tableHasDateDimension($tableId, $dimension)
	{
		foreach ($this->_fetchTableRows($this->definedTables[$tableId]['definitionId']) as $row) {
			if ($row['type'] == 'DATE' && $row['dateDimension'] == $dimension) {
				return true;
			}
		}
		return false;
	}

	public function tableDateDimensions($tableId)
	{
		$result = array();
		foreach ($this->_fetchTableRows($this->definedTables[$tableId]['definitionId']) as $row) {
			if ($row['type'] == 'DATE' && $row['dateDimension']) {
				$result[] = $row['dateDimension'];
			}
		}
		return $result;
	}

	public function setDateDimensionAttribute($dimension, $name, $value)
	{
		$data = array(
			'name' => $dimension,
			$name => $value
		);
		$this->_updateConfigTableRow(self::DATE_DIMENSIONS_TABLE_NAME, $data);
		if (!self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'])
			$this->getDateDimensions();
		self::$_cache[self::DATE_DIMENSIONS_TABLE_NAME]['dimensions'][$dimension][$name] = $value;
	}


	/**
	 * Delete definition for columns removed from data table
	 * @param $tableId
	 * @param $definition
	 * @param $columns
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	public function checkMissingColumns($tableId, $definition, $columns)
	{
		if (!isset($this->definedTables[$tableId])) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		$data = array();
		$headers = null;
		$saveChanges = false;
		foreach ($definition['columns'] as $columnName => $column) {
			if (in_array($columnName, $columns)) {
				$data[] = $column;
			} else {
				$saveChanges = true;
			}
			if (!$headers) {
				$headers = array_keys($column);
			}
		}

		if ($saveChanges) {
			$this->_updateTable($this->definedTables[$tableId]['definitionId'], 'name', $headers, $data);

			$this->setTableAttribute($tableId, 'lastChangeDate', date('c'));
		}
	}


	public function getXml($tableId)
	{
		$this->getDateDimensions();

		$dataTableConfig = $this->getTable($tableId);
		$gdDefinition = $this->getTableDefinition($tableId);
		$this->checkMissingColumns($tableId, $gdDefinition, $dataTableConfig['columns']);
		$dateDimensions = null; // fetch only when needed

		$xml = new \DOMDocument();
		$schema = $xml->createElement('schema');

		$datasetName = !empty($gdDefinition['gdName']) ? $gdDefinition['gdName'] : $gdDefinition['tableId'];
		$name = $xml->createElement('name', $datasetName);
		$schema->appendChild($name);

		if (!isset($dataTableConfig['columns']) || !count($dataTableConfig['columns'])) {
			throw new WrongConfigurationException("Table '$tableId' does not have valid GoodData definition");
		}

		$columns = $xml->createElement('columns');

		$sourceTableInfo = $this->getTable($tableId);
		foreach ($sourceTableInfo['columns'] as $columnName) {

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
			$column->appendChild($xml->createElement('name', $columnDefinition['name']));
			$column->appendChild($xml->createElement('title', (!empty($columnDefinition['gdName']) ? $columnDefinition['gdName']
				: $columnDefinition['name']) . ' (' . $datasetName . ')'));
			$column->appendChild($xml->createElement('ldmType', !empty($columnDefinition['type']) ? $columnDefinition['type'] : 'IGNORE'));
			if ($columnDefinition['type'] != 'FACT') {
				$column->appendChild($xml->createElement('folder', $datasetName));
			}

			if ($columnDefinition['dataType']) {
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
							$refTableDefinition = $this->getTableDefinition($columnDefinition['schemaReference']);
						} catch (WrongConfigurationException $e) {
							throw new WrongConfigurationException("Schema reference '{$columnDefinition['schemaReference']}'"
								. " of column '{$columnDefinition['name']}' does not exist");
						}
						if ($refTableDefinition) {
							$refTableName = isset($refTableDefinition['gdName']) ? $refTableDefinition['gdName'] : $refTableDefinition['tableId'];
							$column->appendChild($xml->createElement('schemaReference', $refTableName));
							$reference = NULL;
							foreach ($refTableDefinition['columns'] as $c) {
								if ($c['type'] == 'CONNECTION_POINT') {
									$reference = $c['name'];
									break;
								}
							}
							if ($reference) {
								$column->appendChild($xml->createElement('reference', $reference));
							} else {
								throw new WrongConfigurationException("Schema reference '{$columnDefinition['schemaReference']}' "
									. "of column '{$columnDefinition['name']}' does not have connection point");
							}
						} else {
							throw new WrongConfigurationException("Schema reference '{$columnDefinition['schemaReference']}' "
								. " of column '{$columnDefinition['name']}' does not exist");
						}
					} else {
						throw new WrongConfigurationException("Schema reference of column '{$columnDefinition['name']}' is empty");
					}

					break;
			}

			$columns->appendChild($column);
		}

		$schema->appendChild($columns);
		$xml->appendChild($schema);

		return $xml->saveXML();
	}


	/**
	 * @return array
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
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
	 * Check configuration table of projects
	 * @throws WrongConfigurationException
	 */
	public function checkProjectsTable()
	{
		$tableId = $this->bucketId . '.' . self::PROJECTS_TABLE_NAME;
		if ($this->_storageApiClient->tableExists($tableId)) {
			$table = $this->_storageApiClient->getTable($tableId);
			self::_checkConfigTable(self::PROJECTS_TABLE_NAME, $table['columns']);
		}
	}



	/**
	 * @return array
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
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
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function checkUsersTable()
	{
		$tableId = $this->bucketId . '.' . self::USERS_TABLE_NAME;
		if ($this->_storageApiClient->tableExists($tableId)) {
			$table = $this->_storageApiClient->getTable($tableId);
			self::_checkConfigTable(self::USERS_TABLE_NAME, $table['columns']);
		}
	}

	/**
	 * @param null $pid
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
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
		if ($this->_storageApiClient->tableExists($tableId)) {
			$table = $this->_storageApiClient->getTable($tableId);
			self::_checkConfigTable(self::PROJECT_USERS_TABLE_NAME, $table['columns']);
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
		self::$_cache[self::PROJECTS_TABLE_NAME][] = $data;
	}

	/**
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
		self::$_cache[self::USERS_TABLE_NAME][] = $data;
	}


	/**
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
		self::$_cache[self::PROJECT_USERS_TABLE_NAME][] = $data;
	}

	/**
	 * Check if pid exists in configuration table of projects
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
	 * Check if email exists in configuration table of users
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
	 * @param string $key
	 * @param string $value
	 * @param null $protected
	 */
	public function setBucketAttribute($key, $value, $protected = null)
	{
		$this->_storageApiClient->setBucketAttribute($this->bucketId, $key, $value, $protected);
		$this->bucketInfo[$key] = $value;
	}


	/**
	 * Drop writer configuration from SAPI
	 */
	public function dropBucket()
	{
		foreach ($this->_storageApiClient->listTables($this->bucketId) as $table) {
			$this->_storageApiClient->dropTable($table['id']);
		}
		$this->_storageApiClient->dropBucket($this->bucketId);
	}

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
		self::$_cache[self::FILTERS_TABLE_NAME] = $data;
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
		self::$_cache[self::FILTERS_PROJECTS_TABLE_NAME][] = $data;
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
		self::$_cache[self::FILTERS_TABLE_NAME] = array();
		$data = $this->getFilters();

		foreach ($data as $k => $v) {
			if ($v['name'] == $name) {
				$data[$k] = array($name, $attribute, $element, $operator, $uri);
				break;
			}
		}

		$this->_updateConfigTable(self::FILTERS_TABLE_NAME, $data, false);
		self::$_cache[self::FILTERS_TABLE_NAME] = $data;
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
	 * Translates attribute name from SAPI form to GD form
	 * Example: out.c-main.users.id -> attr.outcmainusers.id
	 * If GdName is set on SAPI table (GdName = users): out.c-main.users.id -> attr.users.id
	 *
	 * @param $attribute
	 * @return string
	 */
	public function translateAttributeName($attribute)
	{
		$idArr = explode('.', $attribute);
		$tableId = $idArr[0] . '.' . $idArr[1] . '.' . $idArr[2];
		$attrName = $idArr[3];

		$tableDef = $this->getTableDefinition($tableId);

		$tableName = $tableId;
		if (isset($tableDef['gdName'])) {
			$tableName = $tableDef['gdName'];
		}

		return strtolower('attr.' . preg_replace('/[^a-z\d ]/i', '', $tableName) . '.' . $attrName);
	}



	protected function _createConfigTable($tableName)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		return $this->_createTable(
			$this->bucketId . '.' . $tableName,
			self::$_tables[$tableName]['primaryKey'],
			self::$_tables[$tableName]['columns'],
			self::$_tables[$tableName]['indices']);
	}

	protected function _updateConfigTable($tableName, $data, $incremental = true)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		return $this->_saveTable(
			$this->bucketId . '.' . $tableName,
			self::$_tables[$tableName]['primaryKey'],
			self::$_tables[$tableName]['columns'],
			$data,
			$incremental,
			true,
			self::$_tables[$tableName]['indices']
		);
	}

	protected function _updateConfigTableRow($tableName, $data)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		return $this->_updateTableRow(
			$this->bucketId . '.' . $tableName,
			self::$_tables[$tableName]['primaryKey'],
			$data,
			self::$_tables[$tableName]['indices']
		);
	}

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
	 * @param $tableName
	 * @return array|bool
	 */
	public function getConfigTable($tableName)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		$tableId = $this->bucketId . '.' . $tableName;
		if ($this->_storageApiClient->tableExists($tableId)) {
			self::$_cache[$tableName] = $this->_fetchTableRows($tableId);

			if (count(self::$_cache[$tableName])) {
				self::_checkConfigTable($tableName, array_keys(current(self::$_cache[$tableName])));
			}
		} else {
			$this->_createConfigTable($tableName);
			self::$_cache[$tableName] = array();
		}

		return self::$_cache[$tableName];
	}
}
