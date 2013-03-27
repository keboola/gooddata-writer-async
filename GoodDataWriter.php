<?php
/**
 * GoodDataWriter.php
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-03-14
 */

namespace Keboola\GoodDataWriterBundle;

use Monolog\Logger;
use Syrup\ComponentBundle\Component\Component;
use Symfony\Component\HttpFoundation\Request;
use Keboola\StorageApi\Table as StorageApiTable,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Exception as StorageApiException,
	Keboola\StorageApi\Config\Reader,
	Keboola\Csv\CsvFile,
	Keboola\Csv\Exception as CsvFileException;
use Keboola\GoodDataWriterBundle\Exception\WrongParametersException,
	Keboola\GoodDataWriterBundle\Exception\WrongConfigurationException;

class GoodDataWriter extends Component
{
	const JOBS_TABLE_ID = 'in.c-wr-gooddata.jobs';
	const PROJECTS_TABLE_NAME = 'projects';
	const USERS_TABLE_NAME = 'users';
	const PROJECT_USERS_TABLE_NAME = 'project_users';

	protected $_roles = array(
		'admin' => 'adminRole',
		'editor' => 'editorRole',
		'readOnly' => 'readOnlyUserRole',
		'dashboardOnly' => 'dashboardOnlyRole'
	);

	protected $_name = 'gooddata';
	protected $_prefix = 'wr';

	/**
	 * @var string
	 */
	public $tmpDir;
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
	public $bucketId;
	/**
	 * @var array
	 */
	public $bucketConfig;
	/**
	 * @var array
	 */
	public $tokenInfo;
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
	/**
	 * @var string
	 */
	public $backendUrl;
	/**
	 * @var StorageApiClient
	 */
	private $_sharedStorageApi;


	/**
	 * Init Writer
	 * @param $params
	 * @throws Exception\WrongParametersException
	 */
	private function _init($params)
	{
		// Init params
		if (isset($params['writerId'])) {
			$this->writerId = $params['writerId'];
		} else {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}
		$this->tokenInfo = $this->_storageApi->verifyToken();
		$this->projectId = $this->tokenInfo['owner']['id'];

		// Get configuration bucket id
		$this->bucketId = $this->_configurationBucket($this->writerId);
		if (!$this->bucketId) {
			throw new WrongParametersException(sprintf('WriterId \'%s\' does not exist.', $this->writerId));
		}

		// Get configuration
		Reader::$client = $this->_storageApi;
		$this->bucketConfig = Reader::read($this->bucketId, null, false);
		$this->backendUrl = !empty($this->bucketConfig['gd']['backendUrl']) ? $this->bucketConfig['gd']['backendUrl'] : null;

		// Init temp directory
		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		if (!file_exists($tmpDir)) {
			mkdir($tmpDir);
		}
		$this->tmpDir = sprintf('%s/%s-%s-%s/', $tmpDir, $this->_storageApi->token, $this->bucketId, uniqid());
		if (!file_exists($this->tmpDir)) {
			mkdir($this->tmpDir);
		}

		$request = Request::createFromGlobals();
		$url = null;
		if ($request->headers->has('X-StorageApi-Url')) {
			$url = $request->headers->get('X-StorageApi-Url');
		}
		$this->_sharedStorageApi = new StorageApiClient($this->_container->getParameter('gd.shared_token'), $url);
	}

	public function __destruct()
	{
		system('rm -rf ' . $this->tmpDir);
	}


	/**
	 * Check configuration table of projects
	 * @throws Exception\WrongConfigurationException
	 */
	private function _prepareProjects()
	{
		$csvFile = $this->tmpDir . 'projects.csv';
		try {
			$this->_storageApi->exportTable($this->bucketId . '.' . self::PROJECTS_TABLE_NAME, $csvFile);
		} catch (StorageApiException $e) {
			$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECTS_TABLE_NAME);
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
	 * @throws Exception\WrongConfigurationException
	 */
	private function _prepareUsers()
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
	 * @throws Exception\WrongConfigurationException
	 */
	private function _prepareProjectUsers()
	{
		$csvFile = $this->tmpDir . 'project_users.csv';
		try {
			$this->_storageApi->exportTable($this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME, $csvFile);
		} catch (StorageApiException $e) {
			$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME);
			$table->setHeader(array('id', 'pid', 'email', 'role'));
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


	public function getWriters()
	{
		$writers = array();
		foreach ($this->_storageApi->listBuckets() as $bucket) {
			$writerId = false;
			$foundWriterType = false;
			if (isset($bucket['attributes']) && is_array($bucket['attributes'])) foreach($bucket['attributes'] as $attribute) {
				if ($attribute['name'] == 'writerId') {
					$writerId = $attribute['value'];
				}
				if ($attribute['name'] == 'writer') {
					$foundWriterType = $attribute['value'] == $this->_name;
				}
			}
			if ($writerId && $foundWriterType) {
				$writers[] = array(
					'id' => $writerId,
					'bucket' => $bucket['id']
				);
				break;
			}
		}

		return array('writers' => $writers);
	}

	/*public function postWriters($params)
	{
		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}
		if ($this->_configurationBucket($params['writerId'])) {
			throw new WrongParametersException('Writer with id \'writerId\' already exists');
		}

		$tokenInfo = $this->_storageApi->verifyToken();
		$projectId = $this->tokenInfo['owner']['id'];

		$this->_storageApi->createBucket('wr-gooddata-' . $params['writerId'], 'sys',
			'GoodData Writer ' . $params['writerId']);

	}*/


	/**
	 * List projects from configuration
	 * @param $params
	 * @return array
	 */
	public function getProjects($params)
	{
		$this->_init($params);
		$this->_prepareProjects();

		$projects = array();
		$header = true;
		foreach ($this->projectsCsv as $p) {
			if (!$header) {
				$projects[] = array(
					'pid' => $p[0],
					'active' => $p[1]
				);
			} else {
				$header = false;
			}
		}
		return $projects;
	}


	/**
	 * List users from configuration
	 * @param $params
	 * @return array
	 */
	public function getUsers($params)
	{
		$this->_init($params);
		$this->_prepareUsers();

		$users = array();
		$header = true;
		foreach ($this->usersCsv as $u) {
			if (!$header) {
				$users[] = array(
					'email' => $u[0],
					'uri' => $u[1]
				);
			} else {
				$header = false;
			}
		}
		return $users;
	}


	/**
	 * List project users from configuration
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getProjectUsers($params)
	{
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is required");
		}

		$this->_init($params);
		$this->_prepareProjectUsers();

		$users = array();
		$header = true;
		foreach ($this->projectUsersCsv as $u) {
			if (!$header && $u[1] == $params['pid']) {
				$users[] = array(
					'email' => $u[2],
					'role' => $u[3]
				);
			} else {
				$header = false;
			}
		}
		return $users;
	}


	/**
	 * Clone project
	 * @param $params
	 * @throws Exception\WrongConfigurationException
	 * @throws \Exception|Exception\RestApiException
	 * @return array
	 */
	public function postProjects($params)
	{
		$command = 'cloneProject';
		$createdTime = time();


		// Init parameters
		$this->_init($params);
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->_container->getParameter('gd.access_token');
		$projectName = !empty($params['name']) ? $params['name']
			: sprintf($this->_container->getParameter('gd.project_name'), $this->tokenInfo['owner']['name'], $this->writerId);
		$this->_prepareProjects();
		$mainProject = $this->projectsCsv->current();
		if (!$mainProject[1]) {
			throw new WrongConfigurationException('Main project is not active, check projects configuration table');
		}


		$restApi = new GoodData\RestApi($this->backendUrl, $this->_log);
		$jobId = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'pidSource' => $mainProject[1]
			)
		));

		try {
			$gdWriteStartTime = time();

			$restApi->login($this->_container->getParameter('gd.username'), $this->_container->getParameter('gd.password'));

			// Get user uri if not set
			if (empty($this->bucketConfig['gd']['userUri'])) {
				$userUri = $restApi->userUri($this->bucketConfig['gd']['username'], $this->_container->getParameter('gd.domain'));
				$this->_storageApi->setBucketAttribute($this->bucketId, 'gd.userUri', $userUri);
				$this->bucketConfig['gd']['userUri'] = $userUri;
			}

			$projectPid = $restApi->createProject($projectName, $accessToken);

			$mainProject = $this->projectsCsv->current();
			$restApi->cloneProject($mainProject[0], $projectPid);

			$restApi->addUserToProject($this->bucketConfig['gd']['userUri'], $projectPid);
			$this->_addProjectToConfiguration($projectPid);

			$logUrl = $this->_uploadLog('calls-' . $jobId, $restApi->callsLog());
			$this->_finishJob($jobId, 'success', array(
				'gdWriteStartTime' => date('c', $gdWriteStartTime),
				'result' => array('pid' => $projectPid)
			), $logUrl);

			return array(
				'pid' => $projectPid
			);

		} catch (Exception\UnauthorizedException $e) {
			$this->_finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw new WrongConfigurationException('Clone project failed');
		} catch (Exception\RestApiException $e) {
			$this->_finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw $e;
		}

	}


	/**
	 * Add User to Project
	 * @param $params
	 * @return array
	 * @throws Exception\WrongConfigurationException
	 * @throws Exception\WrongParametersException
	 * @throws \Exception|Exception\RestApiException
	 */
	public function postProjectUsers($params)
	{
		$command = 'addUserToProject';
		$createdTime = time();


		// Init parameters
		if (empty($params['email'])) {
			throw new WrongParametersException("Parameter 'email' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is missing");
		}
		if (empty($params['role'])) {
			throw new WrongParametersException("Parameter 'role' is missing");
		}
		if (!in_array($params['role'], array_keys($this->_roles))) {
			throw new WrongParametersException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', array_keys($this->_roles)));
		}
		$this->_init($params);
		$this->_prepareProjectUsers();
		$params['domain'] = $this->_container->getParameter('gd.domain');


		$restApi = new GoodData\RestApi($this->backendUrl, $this->_log);
		$jobId = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params
		));

		try {
			$gdWriteStartTime = time();

			$restApi->login($this->_container->getParameter('gd.username'), $this->_container->getParameter('gd.password'));
			$userUri = $restApi->userUri($params['email'], $params['domain']);

			$restApi->login($this->bucketConfig['gd']['username'], $this->bucketConfig['gd']['password']);
			$restApi->addUserToProject($userUri, $params['pid'], $this->_roles[$params['role']]);

			$this->_addProjectUserToConfiguration($params['pid'], $params['email'], $params['role']);

			$logUrl = $this->_uploadLog('calls-' . $jobId, $restApi->callsLog());
			$this->_finishJob($jobId, 'success', array(
				'gdWriteStartTime' => date('c', $gdWriteStartTime)
			), $logUrl);

			return array();

		} catch (Exception\UnauthorizedException $e) {
			$this->_finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw new WrongConfigurationException('Add user to project failed');
		} catch (Exception\RestApiException $e) {
			$this->_finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw $e;
		}

	}


	/**
	 * Create user
	 * @param $params
	 * @throws Exception\WrongConfigurationException
	 * @throws Exception\WrongParametersException
	 * @throws \Exception|Exception\RestApiException
	 * @return array
	 */
	public function postUsers($params)
	{
		$command = 'createUser';
		$createdTime = time();


		// Init parameters
		if (empty($params['firstName'])) {
			throw new WrongParametersException("Parameter 'firstName' is missing");
		}
		if (empty($params['lastName'])) {
			throw new WrongParametersException("Parameter 'lastName' is missing");
		}
		if (empty($params['email'])) {
			throw new WrongParametersException("Parameter 'email' is missing");
		}
		if (empty($params['password'])) {
			throw new WrongParametersException("Parameter 'password' is missing");
		}
		$params['domain'] = $this->_container->getParameter('gd.domain');
		$this->_init($params);
		$this->_prepareUsers();


		$restApi = new GoodData\RestApi($this->backendUrl, $this->_log);
		$jobId = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params
		));

		try {
			$gdWriteStartTime = time();

			$restApi->login($this->_container->getParameter('gd.username'), $this->_container->getParameter('gd.password'));

			$userUri = $restApi->createUserInDomain($params['domain'], $params['email'], $params['password'], $params['firstName'], $params['lastName']);
			$this->_addUserToConfiguration($params['email'], $userUri);

			$logUrl = $this->_uploadLog('calls-' . $jobId, $restApi->callsLog());
			$this->_finishJob($jobId, 'success', array(
				'gdWriteStartTime' => date('c', $gdWriteStartTime),
				'result' => array('uri' => $userUri)
			), $logUrl);

		} catch (Exception\UnauthorizedException $e) {
			$this->_finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw new WrongConfigurationException('Create user failed');
		} catch (Exception\RestApiException $e) {
			$this->_finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw $e;
		}

	}


	/**
	 * Find configuration bucket for writerId
	 * @param $writerId
	 * @return bool
	 */
	protected function _configurationBucket($writerId)
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
					$foundWriterType = $attribute['value'] == $this->_name;
				}
			}
			if ($foundWriterName && $foundWriterType) {
				$configurationBucket = $bucket['id'];
				break;
			}
		}
		return $configurationBucket;
	}

	protected function _addProjectToConfiguration($pid)
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

	protected function _addUserToConfiguration($email, $uri)
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

	protected function _addProjectUserToConfiguration($pid, $email, $role)
	{
		$data = array(
			'id' => $pid . $email,
			'pid' => $pid,
			'email' => $email,
			'role' => $role
		);
		$table = new StorageApiTable($this->_storageApi, $this->bucketId . '.' . self::PROJECT_USERS_TABLE_NAME);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}


	protected function _createJob($params)
	{
		$tokenInfo = $this->_storageApi->verifyToken();
		$jobInfo = array(
			'runId' => $this->_storageApi->getRunId(),
			'projectId' => $this->projectId,
			'writerId' => $this->writerId,
			'tokenId' => $tokenInfo['id'],
			'tokenDesc' => $tokenInfo['description'],
			'tokenOwnerName' => $tokenInfo['owner']['name'],
			'initializedBy' => null,
			'createdTime' => null,
			'startTime' => null,
			'endTime' => null,
			'backendUrl' => $this->backendUrl,
			'pid' => null,
			'command' => null,
			'dataset' => null,
			'xmlFile' => null,
			'csvFile' => null,
			'parameters' => null,
			'result' => null,
			'gdWriteStartTime' => null,
			'gdWriteBytes' => null,
			'status' => 'waiting',
			'log' => null
		);
		$jobInfo = array_merge($jobInfo, $params);
		$jobId = $this->_storageApi->generateId();
		$this->_updateJobs($jobId, $jobInfo);

		return $jobId;
	}

	protected function _finishJob($jobId, $status, $params, $logUrl)
	{
		$params = array_merge($params, array(
			'status' => $status,
			'log' => $logUrl
		));

		$this->_updateJobs($jobId, $params);

		$params = array_merge($params, array(
			'projectId' => $this->projectId,
			'writerId' => $this->writerId,
			'job' => $jobId,
			'runId' => $this->_storageApi->getRunId()
		));
		$logLevel = ($status == 'error') ? Logger::ERROR : Logger::INFO;
		$this->_log->log($logLevel, $params);
	}

	protected function _finishJobWithError($jobId, $command, $calls, $error)
	{
		$logUrl = $this->_uploadLog('calls-' . $jobId, $calls);
		$exceptionUrl = $this->_uploadLog('exception-' . $jobId, json_encode($error));
		$this->_finishJob($jobId, 'error', array(
			'command' => $command,
			'result' => array('exception' => $exceptionUrl)
		), $logUrl);
	}

	protected function _updateJobs($jobId, $params)
	{
		$jobInfo = array_merge(array('id' => $jobId), $params);

		$table = new StorageApiTable($this->_sharedStorageApi, self::JOBS_TABLE_ID);
		$table->setHeader(array_keys($jobInfo));
		$table->setFromArray(array($jobInfo));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}

	protected function _uploadLog($name, $log)
	{
		$s3Uploader = $this->_container->get('syrup.monolog.s3_uploader');
		return $s3Uploader->uploadString($name, $log);
	}


}
