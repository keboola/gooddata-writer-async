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
	Keboola\StorageApi\Config\Reader,
	Keboola\Csv\CsvFile,
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
	 * @var array
	 */
	private $_projects;
	/**
	 * @var array
	 */
	private $_users;
	/**
	 * @var array
	 */
	private $_projectUsers;
	/**
	 * @var array
	 */
	private $_dateDimensions;
	/**
	 * @var array
	 */
	private $_tablesCache;
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

		if ($this->bucketId && $this->_storageApi->bucketExists($this->bucketId)) {
			Reader::$client = $this->_storageApi;
			$this->bucketInfo = Reader::read($this->bucketId, null, false);

			if (isset($this->bucketInfo['items'])) {
				foreach ($this->bucketInfo['items'] as $tableName => $table) if (isset($table['tableId'])) {
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
			&& (!empty($this->bucketInfo['gd']['userUri']) || !empty($this->bucketInfo['gd']['uid']))
			&& !empty($this->bucketInfo['gd']['password']);

		if (empty($this->bucketInfo['gd']['uid']) && !empty($this->bucketInfo['gd']['userUri'])) {
			if (substr($this->bucketInfo['gd']['userUri'], 0, 21) == '/gdc/account/profile/') {
				$this->bucketInfo['gd']['uid'] = substr($this->bucketInfo['gd']['userUri'], 21);
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

	public function setTableAttribute($tableId, $name, $value)
	{
		if (!isset($this->definedTables[$tableId])) {
			throw new WrongConfigurationException("Definition for table '$tableId' does not exist");
		}

		$this->_storageApi->setTableAttribute($this->definedTables[$tableId]['definitionId'], $name, $value);
	}

	public function getDateDimensions()
	{
		if (!$this->_dateDimensions) {
			$tableId = $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME;
			if ($this->_storageApi->tableExists($tableId)) {
				$data = array();
				$csv = $this->_storageApi->exportTable($tableId);
				foreach (StorageApiClient::parseCsv($csv) as $row) {
					$data[$row['name']] = $row;
				}
				$this->_dateDimensions = $data;

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
		return $this->_dateDimensions;
	}

	public function addDateDimension($name, $includeTime)
	{
		$data = array(
			'name' => $name,
			'includeTime' => $includeTime,
			'lastExportDate' => ''
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::DATE_DIMENSIONS_TABLE_NAME);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->save();
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
					if (count($this->_projects[0]) != 2) {
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
			if (count($table['columns']) != 2) {
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
					if (count($this->_users[0]) != 2) {
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

			array_unshift($this->_users, array(
				'email' => $this->bucketInfo['gd']['username'],
				'uid' => $this->bucketInfo['gd']['uid'],
				'main' => true
			));
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
			if (count($table['columns']) != 2) {
				throw new WrongConfigurationException('Users table in configuration contains invalid number of columns');
			}
			if (!in_array('email', $table['columns']) || !in_array('uid', $table['columns'])) {
				throw new WrongConfigurationException('Users table in configuration appears to be wrongly configured');
			}
		}
	}

	/**
	 * @return array
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	public function getProjectUsers()
	{
		if (!$this->_projectUsers) {
			$tableId = $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME;
			if ($this->_storageApi->tableExists($tableId)) {
				$csv = $this->_storageApi->exportTable($tableId);
				$this->_projectUsers = StorageApiClient::parseCsv($csv);

				if (isset($this->_projectUsers[0])) {
					if (count($this->_projectUsers[0]) != 5) {
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
		return $this->_projectUsers;
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
			if (count($table['columns']) != 5) {
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
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECTS_TABLE_NAME);
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
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::USERS_TABLE_NAME);
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
}