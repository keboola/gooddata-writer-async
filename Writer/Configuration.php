<?php
/**
 * Configuration Wrapper
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

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
	const PROJECT_USERS_TABLE_NAME = 'project_users';
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
	 * @var CsvFile
	 */
	public $projectsCsv;
	/**
	 * @var CsvFile
	 */
	public $usersCsv;
	/**
	 * @var CsvFile
	 */
	public $projectUsersCsv;


	private $_dateDimensionsCache;
	private $_tablesCache;
	private $_tableDefinitionsCache;


	public function __construct($writerId, StorageApiClient $storageApi, $tmpDir)
	{
		$this->writerId = $writerId;
		$this->_storageApi = $storageApi;

		$this->bucketId = $this->configurationBucket($writerId);
		$this->tokenInfo = $this->_storageApi->verifyToken();
		$this->projectId = $this->tokenInfo['owner']['id'];

		if ($this->bucketId && $this->_storageApi->bucketExists($this->bucketId)) {
			Reader::$client = $this->_storageApi;
			$this->bucketInfo = Reader::read($this->bucketId, null, false);

			if (isset($this->bucketInfo['items'])) {
				foreach ($this->bucketInfo['items'] as $tableName => $table) {
					$this->definedTables[$table['tableId']] = array_merge($table, array('definitionId' => $this->bucketId . '.' . $tableName));
				}
				unset($this->bucketInfo['items']);
			}

			$this->backendUrl = !empty($this->bucketInfo['gd']['backendUrl']) ? $this->bucketInfo['gd']['backendUrl'] : null;

			$this->tmpDir = sprintf('%s/%s-%s-%s/', $tmpDir, $this->_storageApi->token, $this->bucketId, uniqid());
			if (!file_exists($this->tmpDir)) {
				mkdir($this->tmpDir);
			}
		}

		$this->_tablesCache = array();
		$this->_tableDefinitionsCache = array();
	}

	public function __destruct()
	{
		system('rm -rf ' . $this->tmpDir);
	}


	/**
	 * Find configuration bucket for writerId
	 * @param $writerId
	 * @return bool
	 */
	public function configurationBucket($writerId)
	{
		$configurationBucket = false;
		foreach ($this->_storageApi->listBuckets() as $bucket) {
			$foundWriterType = false;
			$foundWriterName = false;
			if (isset($bucket['attributes']) && is_array($bucket['attributes'])) foreach($bucket['attributes'] as $attribute) {
				if ($attribute['name'] == 'writerId') {
					$foundWriterName = $attribute['value'] == $writerId;
				}
				if ($attribute['name'] == 'writer') {
					$foundWriterType = $attribute['value'] == self::WRITER_NAME;
				}
			}
			if ($foundWriterName && $foundWriterType) {
				$configurationBucket = $bucket['id'];
				break;
			}
		}
		return $configurationBucket;
	}


	public function checkGoodDataSetup()
	{
		$valid = !empty($this->bucketInfo['gd']['pid'])
			&& !empty($this->bucketInfo['gd']['username'])
			&& !empty($this->bucketInfo['gd']['userUri'])
			&& !empty($this->bucketInfo['gd']['password']);
		if (!$valid) {
			throw new WrongConfigurationException('Writer is missing GoodData configuration');
		}
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

	public function getDateDimensions()
	{
		if (!$this->_dateDimensionsCache) {
			$data = array();
			$csv = $this->_storageApi->exportTable($this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME);
			foreach (StorageApiClient::parseCsv($csv) as $row) {
				$data[$row['name']] = $row;
			}
			$this->_dateDimensionsCache = $data;
		}
		return $this->_dateDimensionsCache;
	}

	public function setDateDimensionAttribute($dimension, $name, $value)
	{
		$data = array(
			'name' => $dimension,
			$name => $value
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME);
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
			$table = new StorageApiTable($this->_storageApi, $this->definedTables[$tableId]['definitionId']);
			$table->setHeader($headers);
			$table->setFromArray($data);
			$table->save();
		}
	}


	public function getXml($tableId)
	{
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
		foreach ($dataTableConfig['columns'] as $columnName) if (isset($gdDefinition['columns'][$columnName])) {
			$columnDefinition = $gdDefinition['columns'][$columnName];

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
						$dateDimensions = array_keys($this->getDateDimensions());
					}
					if (!empty($columnDefinition['dateDimension']) && in_array($columnDefinition['dateDimension'], $dateDimensions)) {
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
	 * Check configuration table of projects
	 * @throws WrongConfigurationException
	 */
	public function prepareProjects()
	{
		if (!$this->projectsCsv) {
			$csvFile = $this->tmpDir . 'projects.csv';
			try {
				$this->_storageApi->exportTable($this->bucketId . '.' . self::PROJECTS_TABLE_NAME, $csvFile);
			} catch (StorageApiException $e) {
				$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECTS_TABLE_NAME, null, 'pid');
				$table->setHeader(array('pid', 'active'));
				$table->save();

				$this->_storageApi->exportTable($this->bucketId . '.' . self::PROJECTS_TABLE_NAME, $csvFile);
			}

			try {
				$this->projectsCsv = new CsvFile($csvFile);
				if ($this->projectsCsv->getColumnsCount() != 2) {
					throw new WrongConfigurationException('Projects table in configuration contains invalid number of columns');
				}
				$headers = $this->projectsCsv->getHeader();
				if ($headers[0] != 'pid' && $headers[1] != 'active') {
					throw new WrongConfigurationException('Projects table in configuration appears to be wrongly configured');
				}

				$this->projectsCsv->next();

			} catch (CsvFileException $e) {
				throw new WrongConfigurationException($e->getMessage());
			}
		}
	}

	/**
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function prepareUsers()
	{
		if (!$this->usersCsv) {
			$csvFile = $this->tmpDir . 'users.csv';
			try {
				$this->_storageApi->exportTable($this->bucketId . '.' . self::USERS_TABLE_NAME, $csvFile);
			} catch (StorageApiException $e) {
				$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::USERS_TABLE_NAME, null, 'email');
				$table->setHeader(array('email', 'uri'));
				$table->save();

				$this->_storageApi->exportTable($this->bucketId . '.' . self::USERS_TABLE_NAME, $csvFile);
			}

			try {
				$this->usersCsv = new CsvFile($csvFile);
				if ($this->usersCsv->getColumnsCount() != 2) {
					throw new WrongConfigurationException('Users table in configuration contains invalid number of columns');
				}
				$headers = $this->usersCsv->getHeader();
				if ($headers[0] != 'email' && $headers[1] != 'uri') {
					throw new WrongConfigurationException('Users table in configuration appears to be wrongly configured');
				}
				$this->usersCsv->next();

			} catch (CsvFileException $e) {
				throw new WrongConfigurationException($e->getMessage());
			}
		}
	}

	/**
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function prepareProjectUsers()
	{
		if (!$this->projectUsersCsv) {
			$csvFile = $this->tmpDir . 'project_users.csv';
			try {
				$this->_storageApi->exportTable($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, $csvFile);
			} catch (StorageApiException $e) {
				$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, null, 'id');
				$table->setHeader(array('id', 'pid', 'email', 'role', 'action'));
				$table->addIndex('pid');
				$table->save();

				$this->_storageApi->exportTable($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, $csvFile);
			}

			try {
				$this->projectUsersCsv = new CsvFile($csvFile);
				if ($this->projectUsersCsv->getColumnsCount() != 5) {
					throw new WrongConfigurationException('Project users table in configuration contains invalid number of columns');
				}
				$headers = $this->projectUsersCsv->getHeader();
				if ($headers[0] != 'id' && $headers[1] != 'pid' && $headers[2] != 'email' && $headers[3] != 'role' && $headers[4] != 'action') {
					throw new WrongConfigurationException('Project users table in configuration appears to be wrongly configured');
				}
				$this->projectUsersCsv->next();

			} catch (CsvFileException $e) {
				throw new WrongConfigurationException($e->getMessage());
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
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECTS_TABLE_NAME);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();

		//@TODO $this->projectsCsv->writeRow(array_values($data));
	}

	/**
	 * @param $email
	 * @param $uri
	 */
	public function saveUserToConfiguration($email, $uri)
	{
		$data = array(
			'email' => $email,
			'uri' => $uri
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::USERS_TABLE_NAME);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();

		//@TODO $this->usersCsv->writeRow(array_values($data));
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
			'id' => $pid . $email . $action . date('c'),
			'pid' => $pid,
			'email' => $email,
			'role' => $role,
			'action' => $action
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();

		//@TODO $this->projectUsersCsv->writeRow(array_values($data));
	}

	/**
	 * Check if pid exists in configuration table of projects
	 * @param $pid
	 * @return bool
	 */
	public function checkProject($pid)
	{
		$this->prepareProjects();
		$firstLine = true;
		foreach ($this->projectsCsv as $project) {
			if (!$firstLine && $project[0] == $pid) return true;
			$firstLine = false;
		}
		return false;
	}

	/**
	 * Check if email exists in configuration table of users
	 * @param $email
	 * @return bool
	 */
	public function user($email)
	{
		$this->prepareUsers();
		$firstLine = true;
		foreach ($this->usersCsv as $user) {
			if (!$firstLine && $user[0] == $email) return array_combine($this->usersCsv->getHeader(), $user);
			$firstLine = false;
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
}