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
	const PROJECTS_TO_DELETE_TABLE_NAME = 'projects_to_delete';
	const USERS_TO_DELETE_TABLE_NAME = 'users_to_delete';


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


	public function checkGoodDataSetup()
	{
		$valid = !empty($this->bucketInfo['gd']['pid'])
			&& !empty($this->bucketInfo['gd']['username'])
			&& !empty($this->bucketInfo['gd']['password']);
		if (!$valid) {
			throw new WrongConfigurationException('Writer is missing GoodData configuration');
		}
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
				$table->save();
				$this->_storageApi->markTableColumnAsIndexed($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, 'pid');
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

		//@TODO $this->projectsCsv->writeRow(array_values($data));
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

		//@TODO $this->usersCsv->writeRow(array_values($data));
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
	public function checkUser($email)
	{
		$this->prepareUsers();
		$firstLine = true;
		foreach ($this->usersCsv as $user) {
			if (!$firstLine && $user[0] == $email) return true;
			$firstLine = false;
		}
		return false;
	}
}