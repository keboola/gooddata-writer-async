<?php

namespace Keboola\GoodDataWriterBundle\Writer;

use Keboola\GoodDataWriterBundle\Writer\Queue,
	Keboola\StorageApi\Table as StorageApiTable,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Exception as StorageApiException,
	Keboola\StorageApi\Config\Reader,
	Keboola\Csv\CsvFile,
	Keboola\Csv\Exception as CsvFileException,
	Keboola\GoodDataWriterBundle\Exception\WrongConfigurationException;

class Configuration
{
	const WRITER_NAME = 'gooddata';
	const PROJECTS_TABLE_NAME = 'projects';
	const USERS_TABLE_NAME = 'users';
	const PROJECT_USERS_TABLE_NAME = 'project_users';


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

			$this->backendUrl = !empty($this->bucketInfo['gd']['backendUrl']) ? $this->bucketInfo['gd']['backendUrl'] : null;

			$this->tmpDir = sprintf('%s/%s-%s-%s/', $tmpDir, $this->_storageApi->token, $this->bucketId, uniqid());
			if (!file_exists($this->tmpDir)) {
				mkdir($this->tmpDir);
			}
		}

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

	/**
	 * Check configuration table of projects
	 * @throws WrongConfigurationException
	 */
	public function prepareProjects()
	{
		$csvFile = $this->tmpDir . 'projects.csv';
		try {
			$this->_storageApi->exportTable($this->bucketId . '.' . Configuration::PROJECTS_TABLE_NAME, $csvFile);
		} catch (StorageApiException $e) {
			$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . Configuration::PROJECTS_TABLE_NAME);
			$table->setHeader(array('pid', 'active'));
			$table->save();
			throw new WrongConfigurationException('Projects table in configuration appears to be empty');
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
			if (!$this->projectsCsv->current()) {
				throw new WrongConfigurationException('Projects table in configuration appears to be empty');
			}

		} catch (CsvFileException $e) {
			throw new WrongConfigurationException($e->getMessage());
		}
	}

	/**
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function prepareUsers()
	{
		$csvFile = $this->tmpDir . 'users.csv';
		try {
			$this->_storageApi->exportTable($this->bucketId . '.' . self::USERS_TABLE_NAME, $csvFile);
		} catch (StorageApiException $e) {
			$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::USERS_TABLE_NAME);
			$table->setHeader(array('email', 'uri'));
			$table->save();
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

	/**
	 * Check configuration table of users
	 * @throws WrongConfigurationException
	 */
	public function prepareProjectUsers()
	{
		$csvFile = $this->tmpDir . 'project_users.csv';
		try {
			$this->_storageApi->exportTable($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, $csvFile);
		} catch (StorageApiException $e) {
			$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME);
			$table->setHeader(array('id', 'pid', 'email', 'role', 'action'));
			$table->save();
			$this->_storageApi->markTableColumnAsIndexed($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, 'pid');
		}

		try {
			$this->projectUsersCsv = new CsvFile($csvFile);
			if ($this->projectUsersCsv->getColumnsCount() != 4) {
				throw new WrongConfigurationException('Project users table in configuration contains invalid number of columns');
			}
			$headers = $this->projectUsersCsv->getHeader();
			if ($headers[0] != 'id' && $headers[1] != 'pid' && $headers[1] != 'email' && $headers[1] != 'role') {
				throw new WrongConfigurationException('Project users table in configuration appears to be wrongly configured');
			}
			$this->projectUsersCsv->next();

		} catch (CsvFileException $e) {
			throw new WrongConfigurationException($e->getMessage());
		}
	}


	/**
	 * @param $pid
	 */
	public function addProjectToConfiguration($pid)
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
	}

	/**
	 * @param $email
	 * @param $uri
	 */
	public function addUserToConfiguration($email, $uri)
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
	}


	/**
	 * @param $pid
	 * @param $email
	 * @param $role
	 */
	public function addProjectUserToConfiguration($pid, $email, $role)
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
	}
}