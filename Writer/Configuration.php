<?php
/**
 * Configuration Wrapper
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\StorageApi\Table as StorageApiTable,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Exception as StorageApiException,
	Keboola\StorageApi\Config\Reader,
	Keboola\Csv\CsvFile,
	Keboola\Csv\Exception as CsvFileException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class Configuration
{
	const WRITER_NAME = 'gooddata';
	const PROJECTS_TABLE_NAME = 'projects';
	const USERS_TABLE_NAME = 'users';
	const FILTERS_TABLE_NAME = 'filters';
	const PROJECT_USERS_TABLE_NAME = 'project_users';
	const FILTERS_USERS_TABLE_NAME = 'filters_users';
	const FILTERS_PROJECTS_TABLE_NAME = 'filters_projects';
	const DATE_DIMENSIONS_TABLE_NAME = 'dateDimensions';


	/**
	 * @var StorageApiClient
	 */
	private $_storageApi;
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
	/**
	 * @var string
	 */
	public $tmpDir;


	/**
	 * @var Array
	 */
	private $_projects;
	/**
	 * @var Array
	 */
	private $_users;
	/**
	 * @var Array
	 */
	private $_filters;
	/**
	 * @var Array
	 */
	private $_projectUsers;
	/**
	 * @var Array
	 */
	private $_filtersUsers;
	/**
	 * @var Array
	 */
	private $_filtersProjects;


	private $_dateDimensions;
	private $_dateDimensionsWithUsage;
	private $_tablesCache;

	/**
	 * @var array
	 */
	private $_outputTables;
	/**
	 * @var array
	 */
	private $_tableDefinitionsCache;


	public function __construct($writerId, StorageApiClient $storageApi, $tmpDir)
	{
		$this->writerId = $writerId;
		$this->_storageApi = $storageApi;

		$this->bucketId = $this->configurationBucket($writerId);
		$this->tokenInfo = $this->_storageApi->verifyToken();
		$this->projectId = $this->tokenInfo['owner']['id'];

		$this->definedTables = array();
		if ($this->bucketId && $this->_storageApi->bucketExists($this->bucketId)) {
			Reader::$client = $this->_storageApi;
			$this->bucketInfo = Reader::read($this->bucketId, null, false);

			if (isset($this->bucketInfo['items'])) {
				foreach ($this->bucketInfo['items'] as $tableName => $table ) if (isset($table['tableId'])) {
					$this->definedTables[$table['tableId']] = array_merge($table, array('definitionId' => $this->bucketId . '.' . $tableName));
				}
				unset($this->bucketInfo['items']);
			}

			$this->backendUrl = !empty($this->bucketInfo['gd']['backendUrl']) ? $this->bucketInfo['gd']['backendUrl'] : null;

			$this->tmpDir = $tmpDir . '/' . $this->_storageApi->token . '-' . $this->bucketId . '-' . uniqid();
			if (!file_exists($this->tmpDir)) {
				system('mkdir ' . escapeshellarg($this->tmpDir));
			}
		}

		$this->_tablesCache = array();
		$this->_tableDefinitionsCache = array();
		$this->_outputTables = array();
	}

	public function __destruct()
	{
		system('rm -rf ' . escapeshellarg($this->tmpDir));
	}


	/**
	 * Find configuration bucket for writerId
	 * @param $writerId
	 * @return bool
	 */
	public function configurationBucket($writerId)
	{
		foreach (self::getWriters($this->_storageApi) as $w) {
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

		$this->_storageApi->createBucket('wr-gooddata-' . $writerId, 'sys', 'GoodData Writer Configuration');
		$this->_storageApi->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'writer', self::WRITER_NAME);
		$this->_storageApi->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'writerId', $writerId);
		if ($backendUrl) {
			$this->_storageApi->setBucketAttribute('sys.c-wr-gooddata-' . $writerId, 'gd.backendUrl', $backendUrl);
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
				$this->_storageApi->setBucketAttribute($this->bucketId, 'gd.uid', $this->bucketInfo['gd']['uid']);
				$this->_storageApi->deleteBucketAttribute($this->bucketId, 'gd.userUri');
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
		if (!$this->_outputTables) {
			$this->_outputTables = array();
			foreach ($this->_storageApi->listBuckets() as $bucket) {
				if (substr($bucket['id'], 0, 3) == 'out') {
					foreach ($this->_storageApi->listTables($bucket['id']) as $table) {
						$this->_outputTables[] = $table['id'];
					}
				}
			}
		}
		return $this->_outputTables;
	}

	public function getTable($tableId)
	{
		if (!isset($this->_tablesCache[$tableId])) {
			if (!$this->_storageApi->tableExists($tableId)) {
				throw new WrongConfigurationException("Table '$tableId' does not exist");
			}

			$this->_tablesCache[$tableId] = $this->_storageApi->getTable($tableId);
		}

		return  $this->_tablesCache[$tableId];
	}

	public function getTableForApi($tableId)
	{
		$data = array('columns' => array());
		$tableInfo = $this->getTable($this->definedTables[$tableId]['definitionId']);
		if (isset($tableInfo['attributes'])) foreach ($tableInfo['attributes'] as $attr) {
			if ($attr['name'] == 'export')
				$attr['value'] = (bool)$attr['value'];
			$data[$attr['name']] = $attr['value'];
		}

		$previews = array();
		if ($this->_storageApi->tableExists($tableId)) {
			$tableExportCsv = $this->_storageApi->exportTable($tableId, null, array('limit' => 10));
			foreach(StorageApiClient::parseCsv($tableExportCsv) as $row) {
				foreach ($row as $key => $value) {
					$previews[$key][] = $value;
				}
			}
		}

		$csv = $this->_storageApi->exportTable($this->definedTables[$tableId]['definitionId']);
		$tableDefinition = array();
		foreach (StorageApiClient::parseCsv($csv) as $row) {
			if (!empty($row['dataTypeSize'])) {
				$row['dataTypeSize'] = (int)$row['dataTypeSize'];
			}

			$tableDefinition[$row['name']] = $row;
		}

		$sourceTableInfo = $this->getTable($tableId);
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
		foreach ($this->_storageApi->listTables() as $table) {
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
		if (!isset($this->_tableDefinitionsCache[$tableId])) {
			if (!isset($this->definedTables[$tableId])) {
				throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
			}

			$data = array('columns' => array());

			$tableInfo = $this->getTable($this->definedTables[$tableId]['definitionId']);
			if (isset($tableInfo['attributes'])) foreach ($tableInfo['attributes'] as $attr) {
				$data[$attr['name']] = $attr['value'];
			}

			$csv = $this->_storageApi->exportTable($this->definedTables[$tableId]['definitionId']);
			foreach (StorageApiClient::parseCsv($csv) as $row) {
				$data['columns'][$row['name']] = $row;
			}
			$this->_tableDefinitionsCache[$tableId] = $data;
		}

		return $this->_tableDefinitionsCache[$tableId];
	}

	public function tableIsReferenceable($tableId)
	{
		$csv = $this->_storageApi->exportTable($this->definedTables[$tableId]['definitionId']);
		foreach (StorageApiClient::parseCsv($csv) as $row) {
			if ($row['type'] == 'CONNECTION_POINT') {
				return true;
			}
		}
		return false;
	}

	public function createTableDefinition($tableId)
	{
		if (!isset($this->_tableDefinitionsCache[$tableId])) {
			if (!isset($this->definedTables[$tableId])) {

				$tId = mb_substr($tableId, mb_strlen(StorageApiClient::STAGE_OUT) + 1);
				$bucket = mb_substr($tId, 0, mb_strpos($tId, '.'));
				$tableName = mb_substr($tId, mb_strpos($tId, '.')+1);

				$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . $bucket . '_' . $tableName, null, 'name');
				$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
					'format', 'dateDimension', 'sortLabel', 'sortOrder'));
				$table->setAttribute('tableId', $tableId);
				$table->setAttribute('lastChangeDate', null);
				$table->setAttribute('lastExportDate', null);
				$table->save();

				$this->definedTables[$tableId] = array(
					'tableId' => $tableId,
					'gdName' => null,
					'lastChangeDate' => null,
					'lastExportDate' => null,
					'definitionId' => $this->bucketId . '.' . $bucket . '_' . $tableName
				);

			}
		}
	}

	public function saveColumnDefinition($tableId, $data)
	{
		if (!isset($this->definedTables[$tableId])) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		$table = new StorageApiTable($this->_storageApi, $this->definedTables[$tableId]['definitionId'], null, 'name');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->setPartial(true);
		$table->save();
	}

	public function setTableAttribute($tableId, $name, $value)
	{
		if (!isset($this->definedTables[$tableId])) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		$this->definedTables[$tableId][$name] = $value;
		$this->_storageApi->setTableAttribute($this->definedTables[$tableId]['definitionId'], $name, $value);
	}

	public function getDateDimensions($usage = false)
	{
		if ((!$usage && !$this->_dateDimensions) || ($usage && !$this->_dateDimensionsWithUsage)) {
			$tableId = $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME;
			if ($this->_storageApi->tableExists($tableId)) {
				if ($usage) {
					$usedIn = array();
					foreach (array_keys($this->definedTables) as $tId) {
						foreach ($this->tableDateDimensions($tId) as $dim) {
							if (!isset($usedIn[$dim])) {
								$usedIn[$dim] = array();
							}
							$usedIn[$dim][] = $tId;
						}
					}
				}

				$data = array();
				$csv = $this->_storageApi->exportTable($tableId);
				foreach (StorageApiClient::parseCsv($csv) as $row) {
					$row['includeTime'] = (bool)$row['includeTime'];
					if ($usage) {
						$row['usedIn'] = isset($usedIn[$row['name']]) ? $usedIn[$row['name']] : array();
					}
					$data[$row['name']] = $row;
				}
				if ($usage) {
					$this->_dateDimensionsWithUsage = $data;
				} else {
					$this->_dateDimensions = $data;
				}

				if (isset($this->_dateDimensions[0])) {
					if (count($this->_dateDimensions[0]) != 3) {
						throw new WrongConfigurationException('Date Dimensions table in configuration contains invalid number of columns');
					}
					if (!isset($this->_dateDimensions[0]['name']) || !isset($this->_dateDimensions[0]['includeTime'])
						|| !isset($this->_dateDimensions[0]['lastExportDate'])) {
						throw new WrongConfigurationException('Date Dimensions table in configuration appears to be wrongly configured');
					}
				}
			} else {
				$table = new StorageApiTable($this->_storageApi, $tableId, null, 'name');
				$table->setHeader(array('name', 'includeTime', 'lastExportDate'));
				$table->save();
				$this->_dateDimensions = array();
			}
		}
		return $usage ? $this->_dateDimensionsWithUsage : $this->_dateDimensions;
	}

	public function addDateDimension($name, $includeTime)
	{
		$data = array(
			'name' => $name,
			'includeTime' => $includeTime,
			'lastExportDate' => ''
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME, null, 'name');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->save();
	}

	public function deleteDateDimension($name)
	{
		$data = array();
		foreach ($this->getDateDimensions() as $dimension) {
			if ($dimension['name'] != $name) {
				$data[] = $dimension;
			} else {
				if (!empty($dimension['lastExportDate'])) {
					throw new WrongConfigurationException(sprintf('Date Dimension %s has been exported to GoodData and cannot be deleted this way.', $name));
				}
				foreach (array_keys($this->definedTables) as $tableId) {
					if ($this->tableHasDateDimension($tableId, $name)) {
						throw new WrongConfigurationException(sprintf('Date Dimension %s is used in dataset %s and cannot be deleted.', $name, $tableId));
					}
				}
			}
		}
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME, null, 'name');
		$table->setHeader(array('name', 'includeTime', 'lastExportDate'));
		$table->setFromArray($data);
		$table->setIncremental(false);
		$table->save();
	}

	public function tableHasDateDimension($tableId, $dimension)
	{
		$csv = $this->_storageApi->exportTable($this->definedTables[$tableId]['definitionId']);
		foreach (StorageApiClient::parseCsv($csv) as $row) {
			if ($row['type'] == 'DATE' && $row['dateDimension'] == $dimension) {
				return true;
			}
		}
		return false;
	}

	public function tableDateDimensions($tableId)
	{
		$result = array();
		$csv = $this->_storageApi->exportTable($this->definedTables[$tableId]['definitionId']);
		foreach (StorageApiClient::parseCsv($csv) as $row) {
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
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME, null, 'name');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
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
			$table = new StorageApiTable($this->_storageApi, $this->definedTables[$tableId]['definitionId'], null, 'name');
			$table->setHeader($headers);
			$table->setFromArray($data);
			$table->save();
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
							$column->appendChild($xml->createElement('schemaReference', $refTableDefinition['gdName']));
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
		if (!$this->_projects) {
			$tableId = $this->bucketId . '.' . self::PROJECTS_TABLE_NAME;
			if ($this->_storageApi->tableExists($tableId)) {
				$csv = $this->_storageApi->exportTable($tableId);
				$this->_projects = StorageApiClient::parseCsv($csv);

				if (isset($this->_projects[0])) {
					if (count($this->_projects[0]) < 2) {
						throw new WrongConfigurationException('Projects table in configuration contains invalid number of columns');
					}
					if (!isset($this->_projects[0]['pid']) || !isset($this->_projects[0]['active'])) {
						throw new WrongConfigurationException('Projects table in configuration appears to be wrongly configured');
					}
				}
			} else {
				$table = new StorageApiTable($this->_storageApi, $tableId, null, 'pid');
				$table->setHeader(array('pid', 'active'));
				$table->save();
				$this->_projects = array();
			}

			if (isset($this->bucketInfo['gd']['pid']))
				array_unshift($this->_projects, array('pid' => $this->bucketInfo['gd']['pid'], 'active' => true, 'main' => true));
		}
		return $this->_projects;
	}

	/**
	 * Check configuration table of projects
	 * @throws WrongConfigurationException
	 */
	public function checkProjectsTable()
	{
		$tableId = $this->bucketId . '.' . self::PROJECTS_TABLE_NAME;
		if ($this->_storageApi->tableExists($tableId)) {
			$table = $this->_storageApi->getTable($tableId);
			if (count($table['columns']) < 2) {
				throw new WrongConfigurationException('Projects table in configuration contains invalid number of columns');
			}
			if (!in_array('pid', $table['columns']) || !in_array('active', $table['columns'])) {
				throw new WrongConfigurationException('Projects table in configuration appears to be wrongly configured');
			}
		}
	}



	/**
	 * @return array
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	public function getUsers()
	{
		if (!$this->_users) {
			$tableId = $this->bucketId . '.' . self::USERS_TABLE_NAME;
			if ($this->_storageApi->tableExists($tableId)) {
				$csv = $this->_storageApi->exportTable($tableId);
				$this->_users = StorageApiClient::parseCsv($csv);

				if (isset($this->_users[0])) {
					if (count($this->_users[0]) < 2) {
						throw new WrongConfigurationException('Users table in configuration contains invalid number of columns');
					}
					if (!isset($this->_users[0]['email']) || !isset($this->_users[0]['uid'])) {
						throw new WrongConfigurationException('Users table in configuration appears to be wrongly configured');
					}
				}
			} else {
				$table = new StorageApiTable($this->_storageApi, $tableId, null, 'email');
				$table->setHeader(array('email', 'uid'));
				$table->save();
				$this->_users = array();
			}

			if (isset($this->bucketInfo['gd']['username']) && isset($this->bucketInfo['gd']['uid'])) {
				array_unshift($this->_users, array(
					'email' => $this->bucketInfo['gd']['username'],
					'uid' => $this->bucketInfo['gd']['uid'],
					'main' => true
				));
			}
		}
		return $this->_users;
	}

	/**
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function checkUsersTable()
	{
		$tableId = $this->bucketId . '.' . self::USERS_TABLE_NAME;
		if ($this->_storageApi->tableExists($tableId)) {
			$table = $this->_storageApi->getTable($tableId);
			if (count($table['columns']) < 2) {
				throw new WrongConfigurationException('Users table in configuration contains invalid number of columns');
			}
			if (!in_array('email', $table['columns']) || !in_array('uid', $table['columns'])) {
				throw new WrongConfigurationException('Users table in configuration appears to be wrongly configured');
			}
		}
	}

	/**
	 * @param null $pid
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 * @return array
	 */
	public function getProjectUsers($pid = null)
	{
		if (!$this->_projectUsers) {
			$tableId = $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME;
			if ($this->_storageApi->tableExists($tableId)) {
				$csv = $this->_storageApi->exportTable($tableId);
				$this->_projectUsers = StorageApiClient::parseCsv($csv);

				if (isset($this->_projectUsers[0])) {
					if (count($this->_projectUsers[0]) < 5) {
						throw new WrongConfigurationException('Project Users table in configuration contains invalid number of columns');
					}
					if (!isset($this->_projectUsers[0]['id']) || !isset($this->_projectUsers[0]['pid']) || !isset($this->_projectUsers[0]['email'])
						|| !isset($this->_projectUsers[0]['role']) || !isset($this->_projectUsers[0]['action'])) {
						throw new WrongConfigurationException('Project Users table in configuration appears to be wrongly configured');
					}
				}
			} else {
				$table = new StorageApiTable($this->_storageApi, $tableId, null, 'id');
				$table->setHeader(array('id', 'pid', 'email', 'role', 'action'));
				$table->addIndex('pid');
				$table->save();
				$this->_projectUsers = array();
			}

			array_unshift($this->_projectUsers, array(
				'id' => 0,
				'pid' => $this->bucketInfo['gd']['pid'],
				'email' => $this->bucketInfo['gd']['username'],
				'role' => 'admin',
				'action' => 'add',
				'main' => true
			));
		}

		$result = $this->_projectUsers;
		if ($pid) {
			$result = array();
			foreach ($this->_projectUsers as $u) {
				if ($u['pid'] == $pid) {
					$result[] = array(
						'email' => $u['email'],
						'role' => $u['role']
					);
				}
			}
		}

		return $result;
	}

	/**
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function checkProjectUsersTable()
	{
		$tableId = $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME;
		if ($this->_storageApi->tableExists($tableId)) {
			$table = $this->_storageApi->getTable($tableId);
			if (count($table['columns']) < 5) {
				throw new WrongConfigurationException('Project Users table in configuration contains invalid number of columns');
			}
			if (!in_array('id', $table['columns']) || !in_array('pid', $table['columns']) || !in_array('email', $table['columns'])
				|| !in_array('role', $table['columns']) || !in_array('action', $table['columns'])) {
				throw new WrongConfigurationException('Project Users table in configuration appears to be wrongly configured');
			}
		}
	}


	/**
	 * @param $pid
	 */
	public function saveProjectToConfiguration($pid)
	{
		$data = array(
			'pid' => $pid,
			'active' => 1
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECTS_TABLE_NAME, null, 'pid');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();

		$this->_projects[] = $data;
	}

	/**
	 * @param $email
	 * @param $uid
	 */
	public function saveUserToConfiguration($email, $uid)
	{
		$data = array(
			'email' => $email,
			'uid' => $uid
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::USERS_TABLE_NAME, null, 'email');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();

		$this->_users[] = $data;
	}


	/**
	 * @param $pid
	 * @param $email
	 * @param $role
	 */
	public function saveProjectUserToConfiguration($pid, $email, $role)
	{
		$action = 'add';
		$data = array(
			'id' => sha1($pid . $email . $action . date('c')),
			'pid' => $pid,
			'email' => $email,
			'role' => $role,
			'action' => $action
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, null, 'id');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		if (!$this->_storageApi->tableExists($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME)) {
			$table->addIndex('pid');
			$table->addIndex('email');
		}
		$table->save();

		$this->_projectUsers[] = $data;
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
		$this->_storageApi->setBucketAttribute($this->bucketId, $key, $value, $protected);
	}


	/**
	 * Drop writer configuration from SAPI
	 */
	public function dropBucket()
	{
		foreach ($this->_storageApi->listTables($this->bucketId) as $table) {
			$this->_storageApi->dropTable($table['id']);
		}
		$this->_storageApi->dropBucket($this->bucketId);
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
	 * @return array
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	public function getFilters()
	{
		$tableId = $this->bucketId . '.' . self::FILTERS_TABLE_NAME;
		$header = array('name', 'attribute', 'element', 'operator', 'uri');
		if ($this->_storageApi->tableExists($tableId)) {
			$csv = $this->_storageApi->exportTable($tableId);
			$this->_filters = StorageApiClient::parseCsv($csv);

			if (isset($this->_filters[0])) {
				if (count($this->_filters[0]) != count($header)) {
					throw new WrongConfigurationException('Filters table in configuration contains invalid number of columns');
				}
				if (array_keys($this->_filters[0]) != $header) {
					throw new WrongConfigurationException('Filters table in configuration appears to be wrongly configured');
				}
			}
		} else {
			$table = new StorageApiTable($this->_storageApi, $tableId, null, $header[0]);
			$table->setFromArray(array($header), true);
			$table->save();
			$this->_filters = array();
		}

		return $this->_filters;
	}


	/**
	 * @return array
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	public function getFiltersUsers()
	{
		$tableId = $this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME;
		$header = array('filterName', 'userEmail');
		if ($this->_storageApi->tableExists($tableId)) {
			$csv = $this->_storageApi->exportTable($tableId);
			$this->_filtersUsers = StorageApiClient::parseCsv($csv);

			if (isset($this->_filtersUsers[0])) {
				if (count($this->_filtersUsers[0]) != count($header)) {
					throw new WrongConfigurationException('FiltersUsers table in configuration contains invalid number of columns');
				}
				if (array_keys($this->_filtersUsers[0]) != $header) {
					throw new WrongConfigurationException('FiltersUsers table in configuration appears to be wrongly configured');
				}
			}
		} else {
			$table = new StorageApiTable($this->_storageApi, $tableId, null, $header[0]);
			$table->setFromArray(array($header), true);
			$table->save();
			$this->_filtersUsers = array();
		}
		return $this->_filtersUsers;
	}

	/**
	 * @return array
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	public function getFiltersProjects()
	{
		$tableId = $this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME;
		$header = array('filterName', 'pid');
		if ($this->_storageApi->tableExists($tableId)) {
			$csv = $this->_storageApi->exportTable($tableId);
			$this->_filtersProjects = StorageApiClient::parseCsv($csv);

			if (isset($this->_filtersProjects[0])) {
				if (count($this->_filtersProjects[0]) != count($header)) {
					throw new WrongConfigurationException('FiltersUsers table in configuration contains invalid number of columns');
				}
				if (array_keys($this->_filtersProjects[0]) != $header) {
					throw new WrongConfigurationException('FiltersUsers table in configuration appears to be wrongly configured');
				}
			}
		} else {
			$table = new StorageApiTable($this->_storageApi, $tableId, null, $header[0]);
			$table->setFromArray(array($header), true);
			$table->save();
			$this->_filtersProjects = array();
		}
		return $this->_filtersProjects;
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
	public function saveFilterToConfiguration($name, $attribute, $element, $operator, $uri)
	{
		// check for existing name
		foreach ($this->getFilters() as $f) {
			if ($f['name'] == $name) {
				throw new WrongParametersException("Filter of that name already exists.");
			}
		}

		if (is_array($element)) {
			$element = json_encode($element);
		}

		$filter = array(
			'name'      => $name,
			'attribute' => $attribute,
			'element'   => $element,
			'operator'  => $operator,
			'uri'       => $uri
		);

		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::FILTERS_TABLE_NAME);
		$table->setHeader(array_keys($filter));
		$table->setFromArray(array($filter));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();

		$this->_filters[] = $filter;
	}

	public function saveFiltersProjectsToConfiguration($filterName, $pid)
	{
		foreach($this->getFiltersProjects() as $fp) {
			if ($fp['filterName'] == $filterName && $fp['pid'] == $pid) {
				throw new WrongParametersException("Filter " . $filterName . " is already assigned to project " . $pid);
			}
		}

		$data = array(
			'filterName'    => $filterName,
			'pid'           => $pid
		);

		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();

		$this->_filtersProjects[] = $data;
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
		$this->_filters = null;
		$filters = $this->getFilters();

		foreach ($filters as $k => $v) {
			if ($v['name'] == $name) {
				$filters[$k] = array($name, $attribute, $element, $operator, $uri);
				break;
			}
		}

		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::FILTERS_TABLE_NAME);
		$table->setHeader(array('name', 'attribute', 'element', 'operator', 'uri'));
		$table->setFromArray($filters);
		$table->save();

		$this->_filters = $filters;
	}

	/**
	 * @param array $filters
	 * @param $userEmail
	 */
	public function saveFilterUserToConfiguration(array $filters, $userEmail)
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

		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME);
		$table->setHeader(array('filterName', 'userEmail'));
		$table->setFromArray($data);
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}

	public function deleteFilterFromConfiguration($filterUri)
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
			$this->_storageApi->dropTable($this->bucketId . '.' . self::FILTERS_TABLE_NAME);
		} else {
			$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::FILTERS_TABLE_NAME);
			$table->setHeader(array('name','attribute','element','operator','uri'));
			$table->setFromArray($filters);
			$table->save();
		}

		// Update filtersUsers table
		$filtersUsers = array();
		foreach ($this->getFiltersUsers() as $row) {
			if ($row['filterName'] != $filterName) {
				$filtersUsers[] = $row;
			}
		}

		if (empty($filtersUsers)) {
			$this->_storageApi->dropTable($this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME);
		} else {
			$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::FILTERS_USERS_TABLE_NAME);
			$table->setHeader(array('filterName', 'userEmail'));
			$table->setFromArray($filtersUsers);
			$table->save();
		}

		// Update filtersProjects table
		$filtersProjects = array();
		foreach ($this->getFiltersProjects() as $row) {
			if ($row['filterName'] != $filterName) {
				$filtersProjects[] = $row;
			}
		}

		if (empty($filtersProjects)) {
			$this->_storageApi->dropTable($this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME);
		} else {
			$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::FILTERS_PROJECTS_TABLE_NAME);
			$table->setHeader(array('filterName', 'pid'));
			$table->setFromArray($filtersProjects);
			$table->save();
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
}
