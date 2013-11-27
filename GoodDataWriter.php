<?php
/**
 * GoodDataWriter.php
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-03-14
 */

namespace Keboola\GoodDataWriter;

use Guzzle\Common\Exception\InvalidArgumentException;
use Guzzle\Http\Url;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\SSO,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedConfig,
	Keboola\StorageApi\Client as StorageApiClient;
use Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\Exception\WrongParametersException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Syrup\ComponentBundle\Component\Component;
use Monolog\Logger;
use Symfony\Component\HttpFoundation\ResponseHeaderBag,
	Symfony\Component\HttpFoundation\StreamedResponse,
	Symfony\Component\HttpFoundation\Response;
use Syrup\ComponentBundle\Exception\SyrupComponentException;

class GoodDataWriter extends Component
{

	protected $_name = 'gooddata';
	protected $_prefix = 'wr';

	/**
	 * @var Configuration
	 */
	public $configuration;
	/**
	 * @var Writer\SharedConfig
	 */
	public $sharedConfig;

	/**
	 * @var array
	 */
	private $_mainConfig;
	/**
	 * @var Service\S3Client
	 */
	private $_s3Client;
	/**
	 * @var Service\Queue
	 */
	private $_queue;

	/**
	 * Init Writer
	 * @param $params
	 * @throws Exception\WrongParametersException
	 */
	private function _init($params)
	{
		// Init params
		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}

		if (isset($params['queue']) && !in_array($params['queue'], array(SharedConfig::PRIMARY_QUEUE, SharedConfig::SECONDARY_QUEUE))) {
			throw new WrongParametersException('Wrong parameter \'queue\'. Must be one of: ' . SharedConfig::PRIMARY_QUEUE . ', ' . SharedConfig::SECONDARY_QUEUE);
		}

		// Init main temp directory
		$this->_mainConfig = $this->_container->getParameter('gooddata_writer');
		$this->configuration = new Configuration($this->_storageApi, $params['writerId']);

		$this->_s3Client = new Service\S3Client(
			\Aws\S3\S3Client::factory(array(
				'key' => $this->_mainConfig['aws']['access_key'],
				'secret' => $this->_mainConfig['aws']['secret_key'])
			),
			$this->_mainConfig['aws']['s3_bucket'],
			$this->configuration->projectId . '.' . $this->configuration->writerId
		);

		$sqsClient = \Aws\Sqs\SqsClient::factory(array(
			'key' => $this->_mainConfig['aws']['access_key'],
			'secret' => $this->_mainConfig['aws']['secret_key'],
			'region' => $this->_mainConfig['aws']['region']
		));
		$this->_queue = new Service\Queue($sqsClient, $this->_mainConfig['aws']['queue_url']);

		$sharedStorageApi = new StorageApiClient(
			$this->_mainConfig['shared_sapi']['token'],
			$this->_mainConfig['shared_sapi']['url'],
			$this->_mainConfig['user_agent']
		);
		$this->sharedConfig = new Writer\SharedConfig($sharedStorageApi);
	}


	/**
	 * List all configured writers
	 *
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getWriters($params)
	{
		if (isset($params['writerId'])) {
			$this->_init($params);
			if (!$this->configuration->bucketId) {
				throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
			}

			return array('writer' => $this->configuration->bucketInfo());
		} else {
			$this->configuration = new Configuration($this->_storageApi);
			return array('writers' => $this->configuration->getWriters());
		}
	}


	/**
	 * Create new writer with main GoodData project and user
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function postWriters($params)
	{
		$command = 'createWriter';
		$createdTime = time();

		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}
		if (!preg_match('/^[a-zA-z0-9_]+$/', $params['writerId'])) {
			throw new WrongParametersException('Parameter writerId may contain only basic letters, numbers and underscores');
		}

		$this->_init($params);

		$this->configuration->createWriter($params['writerId'], isset($params['backendUrl']) ? $params['backendUrl'] : null);

		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->_mainConfig['gd']['access_token'];
		$projectName = sprintf($this->_mainConfig['gd']['project_name'], $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);


		$batchId = $this->_storageApi->generateId();
		$jobInfo = $this->_createJob(array(
			'batchId' => $batchId,
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName
			)
		));

		if(empty($params['users'])) {
			$this->_enqueue($batchId);

			if (empty($params['wait'])) {
				return array('job' => (int)$jobInfo['id']);
			} else {
				$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
				if (isset($result['job']['result']['pid'])) {
					return array('pid' => $result['job']['result']['pid']);
				} else {
					$e = new JobProcessException('Job failed');
					$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
					throw $e;
				}
			}
		} else {

			$users = explode(',', $params['users']);
			foreach ($users as $user) {
				$this->_createJob(array(
					'batchId' => $batchId,
					'command' => 'addUserToProject',
					'createdTime' => date('c', $createdTime),
					'parameters' => array(
						'email' => $user,
						'role' => 'admin'
					)
				));
			}

			$this->_enqueue($batchId);

			return array('batch' => (int)$batchId);
		}


	}


	/**
	 * Delete writer with projects and users
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function deleteWriters($params)
	{
		$command = 'dropWriter';
		$createdTime = time();

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$this->configuration->updateWriter('toDelete', '1');

		$this->sharedConfig->cancelJobs($this->configuration->projectId, $this->configuration->writerId);

		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array()
		));
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			return $this->_waitForJob($jobInfo['id'], $params['writerId']);
		}
	}


	/***********************
	 * @section Projects
	 */


	/**
	 * List projects from configuration
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getProjects($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		return array('projects' => $this->configuration->getProjects());
	}


	/**
	 * Clone project
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function postProjects($params)
	{
		$command = 'cloneProject';
		$createdTime = time();

		// Init parameters
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->_mainConfig['gd']['access_token'];
		$projectName = !empty($params['name']) ? $params['name']
			: sprintf($this->_mainConfig['gd']['project_name'], $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();


		$bucketInfo = $this->configuration->bucketInfo();
		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'includeData' => empty($params['includeData']) ? 0 : 1,
				'includeUsers' => empty($params['includeUsers']) ? 0 : 1,
				'pidSource' => $bucketInfo['gd']['pid']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['pid'])) {
				return array('pid' => $result['job']['result']['pid']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
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
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		return array('users' => $this->configuration->getProjectUsers($params['pid']));
	}


	/**
	 * Add User to Project
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
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
		$allowedRoles = array_keys(RestApi::$userRoles);
		if (!in_array($params['role'], $allowedRoles)) {
			throw new WrongParametersException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', $allowedRoles));
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();
		$this->configuration->checkUsersTable();
		$this->configuration->checkProjectUsersTable();


		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'email' => $params['email'],
				'pid' => $params['pid'],
				'role' => $params['role'],
				'createUser' => isset($params['createUser']) ? 1 : null
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			return $this->_waitForJob($jobInfo['id'], $params['writerId']);
		}
	}


	/**
	 * Remove User from Project
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function deleteProjectUsers($params)
	{
		$command = 'removeUserFromProject';
		$createdTime = time();

		// Init parameters
		if (empty($params['email'])) {
			throw new WrongParametersException("Parameter 'email' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is missing");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		if (!$this->configuration->isProjectUser($params['email'], $params['pid'])) {
			throw new WrongParametersException(sprintf("Project user '%s' is not configured for the writer", $params['email']));
		}
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();
		$this->configuration->checkProjectUsersTable();

		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params
		));

		$this->_enqueue($jobInfo['batchId']);

		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$jobId = $jobInfo['id'];
			$jobFinished = false;
			do {
				$jobInfo = $this->getJobs(array('jobId' => $jobId, 'writerId' => $params['writerId']));
				if (isset($jobInfo['job']['status']) && ($jobInfo['job']['status'] == 'success' || $jobInfo['job']['status'] == 'error')) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep(30);
			} while(!$jobFinished);

			if ($jobInfo['job']['status'] == 'success') {
				return array();
			} else {
				$e = new JobProcessException('Remove Project User job failed');
				$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
				throw $e;
			}
		}
	}


	/***********************
	 * @section Users
	 */

	/**
	 * List users from configuration
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getUsers($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (isset($params['userEmail'])) {
			$user = $this->configuration->getUser($params['userEmail']);
			return array('user' => $user ? $user : null);
		} else {
			return array('users' => $this->configuration->getUsers());
		}
	}

	/**
	 * Create user
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
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
		if (strlen($params['password']) < 7) {
			throw new WrongParametersException("Parameter 'password' must have at least seven characters");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkUsersTable();


		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'firstName' => $params['firstName'],
				'lastName' => $params['lastName'],
				'email' => $params['email'],
				'password' => $params['password']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['uid'])) {
				return array('uid' => $result['job']['result']['uid']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	public function getSso($params)
	{
		// Init parameters
		if (empty($params['email'])) {
			throw new WrongParametersException("Parameter 'email' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is missing");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!empty($params['pid']) && !$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}

		if (!empty($params['createUser']) && $params['createUser'] = 1) {
			$params['wait'] = 1;
			$this->postUsers($params);
			$this->postProjectUsers($params);
		}

		$user = $this->configuration->getUser($params['email']);
		if (!$user) {
			throw new WrongParametersException("User " . $user . " doesn't exist in writer");
		}

		$sso = new SSO($this->configuration, $this->_mainConfig['gd'], $this->_mainConfig['tmp_path']);

		$targetUrl = '/#s=/gdc/projects/' . $params['pid'];
		if (isset($params['targetUrl'])) {
		    $targetUrl = $params['targetUrl'];
		}

		$validity = (isset($params['validity']))?$params['validity']:86400;

		$ssoLink = $sso->url($targetUrl, $params['email'], $validity);

		if (null == $ssoLink) {
		    throw new SyrupComponentException(500, "Can't generate SSO link. Something is broken.");
		}

		return array(
			'ssoLink' => $ssoLink
		);
	}

	/**
	 * Call GD Api with given query
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function getProxy($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		// query validation
		try {
			if (empty($params['query'])) {
				throw new WrongParametersException("Parameter 'query' is missing");
			}

			// clean url - remove domain
			$query = Url::factory(urldecode($params['query']));

			$url = Url::buildUrl(array(
				'path' => $query->getPath(),
				'query' => $query->getQuery(),
			));
		} catch (InvalidArgumentException $e) {
			throw new WrongParametersException("Wrong value for 'query' parameter given");
		}

		$restApi = new RestApi($this->_log);

		$bucketInfo = $this->configuration->bucketInfo();
		$restApi->setCredentials($bucketInfo['gd']['username'], $bucketInfo['gd']['password']);

		try {
			$return = $restApi->get($url);

			return array(
				'response' => $return
			);
		} catch (RestApiException $e) {
			throw new WrongParametersException($e->getMessage(), $e);
		}
	}

	/***********************
	 * @section Filters
	 */


	/**
	 * Returns list of filters configured in writer
	 * If 'userEmail' parameter is specified, only returns filters for specified user
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function getFilters($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (isset($params['userEmail'])) {
			if (isset($params['pid'])) {
				$filters = $this->configuration->getFiltersForUser($params['userEmail'], $params['pid']);
			} else {
				$filters = $this->configuration->getFiltersForUser($params['userEmail']);
			}
		} else {
			$filters = $this->configuration->getFilters();
		}

		return array('filters' => $filters);
	}

	/**
	 * Create new user filter
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postFilters($params)
	{
		$command = 'createFilter';
		$createdTime = time();

		// Init parameters
		if (empty($params['name'])) {
			throw new WrongParametersException("Parameter 'name' is missing");
		}
		if (empty($params['attribute'])) {
			throw new WrongParametersException("Parameter 'attribute' is missing");
		}
		if (empty($params['element'])) {
			throw new WrongParametersException("Parameter 'element' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is missing");
		}
		if (!isset($params['operator'])) {
			$params['operator'] = '=';
		}

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'name' => $params['name'],
				'attribute' => $params['attribute'],
				'element' => $params['element'],
				'pid' => $params['pid'],
				'operator' => $params['operator']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);

			if (isset($result['job']['result']['uri'])) {
				return array('uri' => $result['job']['result']['uri']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	public function deleteFilters($params)
	{
		$command = 'deleteFilter';
		$createdTime = time();

		// Init parameters
		if (empty($params['uri'])) {
			throw new WrongParametersException("Parameter 'uri' is missing");
		}

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'uri' => $params['uri']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['uri'])) {
				return array('uri' => $result['job']['result']['uri']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	/**
	 * Assign filter to user
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postFiltersUser($params)
	{
		$command = 'assignFiltersToUser';
		$createdTime = time();

		if (empty($params['filters'])) {
			throw new WrongParametersException("Parameter 'filters' is missing");
		}
		if (empty($params['userEmail'])) {
			throw new WrongParametersException("Parameter 'userEmail' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is missing");
		}

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'filters' => $params['filters'],
				'userEmail' => $params['userEmail'],
				'pid' => $params['pid']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['uri'])) {
				return array('uri' => $result['job']['result']['uri']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	/**
	 * Synchronize filters from writer's configuration to GoodData project
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postSyncFilters($params)
	{
		$command = 'syncFilters';
		$createdTime = time();

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'pid' => isset($params['pid']) ? $params['pid'] : null
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);

			if (isset($result['job']['status']) && $result['job']['status'] == 'success') {
				return array('message' => 'filters synced');
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}



	/***********************
	 * @section Data and project structure
	 */

	/**
	 * @param $params
	 * @throws Exception\WrongParametersException
	 */
	public function getXml($params)
	{
		$this->_init($params);

		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (empty($params['tableId'])) {
			throw new WrongParametersException("Parameter 'tableId' is missing");
		}

		$response = new Response($this->configuration->getXml($params['tableId']));
		$response->headers->set('Content-Type', 'application/xml');
		$response->headers->set('Access-Control-Allow-Origin', '*');
		$response->send();
		exit();
	}

	/**
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postUploadTable($params)
	{
		$createdTime = time();

		// Init parameters
		if (empty($params['tableId'])) {
			throw new WrongParametersException("Parameter 'tableId' is missing");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$this->configuration->checkBucketAttributes();
		$this->configuration->getDateDimensions();

		$batchId = $this->_storageApi->generateId();

		$xml = $this->configuration->getXml($params['tableId']);
		$xmlUrl = $this->_s3Client->uploadString(sprintf('batch-%s/%s.xml', $batchId, $params['tableId']), $xml, 'text/xml');

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$jobData = array(
			'batchId' => $batchId,
			'command' => 'uploadTable',
			'dataset' => !empty($tableDefinition['name']) ? $tableDefinition['name'] : $tableDefinition['id'],
			'createdTime' => date('c', $createdTime),
			'xmlFile' => $xmlUrl,
			'parameters' => array(
				'tableId' => $params['tableId']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		);
		if (isset($params['pid'])) {
			$jobData['parameters']['pid'] = $params['pid'];
		}
		if (isset($params['incrementalLoad'])) {
			$jobData['parameters']['incrementalLoad'] = $params['incrementalLoad'];
		}
		if (isset($params['sanitize'])) {
			$jobData['parameters']['sanitize'] = $params['sanitize'];
		}
		$jobInfo = $this->_createJob($jobData);
		$this->_enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			return $this->_waitForJob($jobInfo['id'], $params['writerId']);
		}
	}

	/**
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @throws Exception\WrongConfigurationException
	 */
	public function postUploadProject($params)
	{
		$createdTime = time();

		// Init parameters
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$this->configuration->checkBucketAttributes();
		$this->configuration->getDateDimensions();
		$runId = $this->_storageApi->getRunId();
		$batchId = $this->_storageApi->generateId();

		// Get tables XML and check them for errors
		$tables = array();
		foreach ($this->configuration->getDataSets() as $dataSet) if (!empty($dataSet['export'])) {
			try {
				$xml = $this->configuration->getXml($dataSet['id']);
			} catch (WrongConfigurationException $e) {
				throw new WrongConfigurationException(sprintf('Wrong configuration of table \'%s\': %s', $dataSet['id'], $e->getMessage()));
			}
			$xmlUrl = $this->_s3Client->uploadString(sprintf('batch-%s/%s.xml', $batchId, $dataSet['id']), $xml, 'text/xml');
			$definition = $this->configuration->getDataSet($dataSet['id']);

			$tables[$dataSet['id']] = array(
				'dataset' => !empty($dataSet['name']) ? $dataSet['name'] : $dataSet['id'],
				'tableId' => $dataSet['id'],
				'xml' => $xmlUrl,
				'definition' => $definition['columns']
			);
		}


		// @TODO move the code somewhere else
		// Sort tables for GD export according to their references
		$unsortedTables = array();
		$sortedTables = array();
		$references = array();
		$allTableIds = array_keys($tables);
		foreach ($tables as $tableId => $tableConfig) {
			$unsortedTables[$tableId] = $tableConfig;
			foreach ($tableConfig['definition'] as $c) if ($c['type'] == 'REFERENCE' && !empty($c['schemaReference'])) {
				if (in_array($c['schemaReference'], $allTableIds)) {
					$references[$tableId][] = $c['schemaReference'];
				} else {
					throw new WrongConfigurationException("Schema reference '{$c['schemaReference']}' for table '{$tableId}'does not exist");
				}
			}
		}

		$ttl = 20;
		while (count($unsortedTables)) {
			foreach ($unsortedTables as $tableId => $tableConfig) {
				$areSortedReferences = TRUE;
				if (isset($references[$tableId])) foreach($references[$tableId] as $r) {
					if (!array_key_exists($r, $sortedTables)) {
						$areSortedReferences = FALSE;
					}
				}
				if ($areSortedReferences) {
					$sortedTables[$tableId] = $tableConfig;
					unset($unsortedTables[$tableId]);
				}
			}
			$ttl--;

			if ($ttl <= 0) {
				throw new WrongConfigurationException('Check of references failed with timeout. You probably have a recursion in tables references');
			}
		}

		foreach ($sortedTables as $table) {
			$jobData = array(
				'batchId' => $batchId,
				'runId' => $runId,
				'command' => 'uploadTable',
				'dataset' => $table['dataset'],
				'createdTime' => date('c', $createdTime),
				'xmlFile' => $table['xml'],
				'parameters' => array(
					'tableId' => $table['tableId']
				),
				'queue' => isset($params['queue']) ? $params['queue'] : null
			);
			if (isset($params['pid'])) {
				$jobData['parameters']['pid'] = $params['pid'];
			}
			if (isset($params['incrementalLoad'])) {
				$jobData['parameters']['incrementalLoad'] = $params['incrementalLoad'];
			}
			if (isset($params['sanitize'])) {
				$jobData['parameters']['sanitize'] = $params['sanitize'];
			}
			$this->_createJob($jobData);
		}

		// Execute reports
		$jobData = array(
			'batchId' => $batchId,
			'runId' => $runId,
			'command' => 'executeReports',
			'createdTime' => date('c', $createdTime),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		);
		$this->_createJob($jobData);

		$this->_enqueue($batchId);


		if (empty($params['wait'])) {
			return array('batch' => (int)$batchId);
		} else {
			return $this->_waitForBatch($batchId, $params['writerId']);
		}
	}

	/**
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postExecuteReports($params)
	{
		$command = 'executeReports';
		$createdTime = time();

		// Init parameters
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$this->configuration->checkBucketAttributes();

		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));

		$this->_enqueue($jobInfo['batchId']);

		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['status']) && $result['job']['status'] == 'success') {
				return array('message' => 'Reports executed');
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}

	}



	/***********************
	 * @section UI support
	 */


	/**
	 * Get visual model
	 * @param $params
	 * @throws Exception\WrongParametersException
	 */
	public function getModel($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		// @TODO move the code somewhere else
		if (isset($params['mode']) && $params['mode'] == 'new') {
			$result = array(
				'nodes' => array(),
				'transitions' => array()
			);
			$dimensionsUrl = sprintf('%s/admin/gooddata/dates/project/%s/writer/%s',
				$this->_container->getParameter('storageApi.url'), $this->configuration->projectId, $this->configuration->writerId);
			$tableUrl = sprintf('%s/admin/gooddata/columns/project/%s/writer/%s/table/',
				$this->_container->getParameter('storageApi.url'), $this->configuration->projectId, $this->configuration->writerId);
			foreach ($this->configuration->getDataSets() as $dataSet) if (!empty($dataSet['export'])) {

				$result['nodes'][] = array(
					'node' => $dataSet['id'],
					'label' => !empty($dataSet['name']) ? $dataSet['name'] : $dataSet['id'],
					'type' => 'dataset',
					'link' => $tableUrl . $dataSet['id']
				);

				$definition = $this->configuration->getDataSet($dataSet['id']);
				foreach ($definition['columns'] as $c) {
					if ($c['type'] == 'DATE' && $c['dateDimension']) {

						$result['nodes'][] = array(
							'node' => 'dim.' . $c['dateDimension'],
							'label' => $c['dateDimension'],
							'type' => 'dimension',
							'link' => $dimensionsUrl
						);
						$result['transitions'][] = array(
							'source' => $dataSet['id'],
							'target' => 'dim.' . $c['dateDimension'],
							'type' => 'dimension'
						);

					}
					if ($c['type'] == 'REFERENCE' && $c['schemaReference']) {

						$result['transitions'][] = array(
							'source' => $dataSet['id'],
							'target' => $c['schemaReference'],
							'type' => 'dataset'
						);
					}
				}
			}
		} else {
			$nodes = array();
			$dateDimensions = array();
			$references = array();
			$dataSets = array();
			foreach ($this->configuration->getDataSets() as $dataSet) if (!empty($dataSet['export'])) {
				$dataSets[$dataSet['id']] = !empty($dataSet['name']) ? $dataSet['name'] : $dataSet['id'];
				$nodes[] = $dataSet['id'];
				$definition = $this->configuration->getDataSet($dataSet['id']);
				foreach ($definition['columns'] as $c) {
					if ($c['type'] == 'DATE' && $c['dateDimension']) {
						$dateDimensions[$dataSet['id']] = $c['dateDimension'];
						if (!in_array($c['dateDimension'], $nodes)) $nodes[] = $c['dateDimension'];
					}
					if ($c['type'] == 'REFERENCE' && $c['schemaReference']) {
						if (!isset($references[$dataSet['id']])) {
							$references[$dataSet['id']] = array();
						}
						$references[$dataSet['id']][] = $c['schemaReference'];
					}
				}
			}

			$dataSetIds = array_keys($dataSets);
			$result = array('nodes' => array(), 'links' => array());

			foreach ($nodes as $name) {
				$result['nodes'][] = array(
					'name' => isset($dataSets[$name]) ? $dataSets[$name] : $name,
					'group' => in_array($name, $dataSetIds) ? 'dataset' : 'dimension'
				);
			}
			foreach ($dateDimensions as $dataSet => $date) {
				$result['links'][] = array(
					'source' => array_search($dataSet, $nodes),
					'target' => array_search($date, $nodes),
					'value' => 'dimension'
				);
			}
			foreach ($references as $source => $targets) {
				foreach ($targets as $target) {
					$result['links'][] = array(
						'source' => array_search($source, $nodes),
						'target' => array_search($target, $nodes),
						'value' => 'dataset'
					);
				}
			}
		}

		$response = new Response(json_encode($result));
		$response->headers->set('Content-Type', 'application/json');
		$response->headers->set('Access-Control-Allow-Origin', '*');
		$response->send();
		exit();
	}


	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function getTables($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (isset($params['tableId'])) {
			// Table detail
			return array('table' => $this->configuration->getDataSetForApi($params['tableId']));
		} elseif (isset($params['referenceable'])) {
			return array('tables' => $this->configuration->getDataSetsWithConnectionPoint());
		} else {
			return array('tables' => $this->configuration->getDataSets());
		}
	}

	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postTables($params)
	{
		if (empty($params['tableId'])) {
			throw new WrongParametersException("Parameter 'tableId' is missing");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!in_array($params['tableId'], $this->configuration->getOutputSapiTables())) {
			throw new WrongParametersException(sprintf("Table '%s' does not exist", $params['tableId']));
		}

		$this->configuration->updateDataSetsFromSapi();

		if (isset($params['column'])) {
			$params['column'] = trim($params['column']);

			// Column detail
			$sourceTableInfo = $this->configuration->getSapiTable($params['tableId']);
			if (!in_array($params['column'], $sourceTableInfo['columns'])) {
				throw new WrongParametersException(sprintf("Table '%s' does not exist", $params['tableId']));
			}

			$values = array();
			foreach ($params as $key => $value) if (in_array($key, array('gdName', 'type', 'dataType', 'dataTypeSize',
				'schemaReference', 'reference', 'format', 'dateDimension', 'sortLabel', 'sortOrder'))) {
				$values[$key] = $value;
			}
			if (count($values)) {
				$this->configuration->updateColumnDefinition($params['tableId'], $params['column'], $values);
				$this->configuration->updateDataSetDefinition($params['tableId'], 'lastChangeDate', date('c'));
			}
		} else {
			// Table detail
			$this->configuration->updateDataSetDefinition($params['tableId'], 'lastChangeDate', date('c'));
			if (isset($params['gdName'])) $params['name'] = $params['gdName']; //@TODO remove
			foreach ($params as $key => $value) if (in_array($key, array('name', 'export', 'lastChangeDate', 'lastExportDate', 'sanitize', 'incrementalLoad'))) {
				$this->configuration->updateDataSetDefinition($params['tableId'], $key, $value);
			}
		}

		return array();
	}


	/**
	 * Reset export status of all datasets and dimensions
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postResetExport($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		foreach ($this->configuration->getDataSets() as $dataSet) if (!empty($dataSet['isExported'])) {
			$this->configuration->updateDataSetDefinition($dataSet['id'], 'isExported', 0);
		}

		return array();
	}


	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function getDateDimensions($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		return array('dimensions' => $this->configuration->getDateDimensions(isset($params['usage'])));
	}


	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function deleteDateDimensions($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!isset($params['name'])) {
			throw new WrongParametersException("Parameter 'name' is missing");
		}

		$dimensions = $this->configuration->getDateDimensions();
		if (isset($dimensions[$params['name']])) {
			$this->configuration->deleteDateDimension($params['name']);
			return array();
		} else {
			throw new WrongParametersException(sprintf("Dimension '%s' does not exist", $params['name']));
		}
	}


	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postDateDimensions($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (!isset($params['name'])) {
			throw new WrongParametersException("Parameter 'name' is missing");
		}
		$params['name'] = trim($params['name']);

		$dimensions = $this->configuration->getDateDimensions();
		if (!isset($dimensions[$params['name']])) {
			$this->configuration->saveDateDimension($params['name'], !empty($params['includeTime']));
		}

		return array();
	}



	/***********************
	 * @section Jobs
	 */

	/**
	 * Get Jobs
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getJobs($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (empty($params['jobId'])) {
			$days = isset($params['days']) ? $params['days'] : 7;
			$jobs = $this->sharedConfig->fetchJobs($this->configuration->projectId, $params['writerId'], $days);

			$result = array();
			foreach ($jobs as $job) {
				$result[] = $this->sharedConfig->jobToApiResponse($job, $this->_s3Client);
			}

			return array('jobs' => $result);
		} else {
			if (is_array($params['jobId'])) {
				throw new WrongParametersException("Parameter 'jobId' has to be a number");
			}
			$job = $this->sharedConfig->fetchJob($params['jobId'], $this->configuration->writerId, $this->configuration->projectId);
			if (!$job) {
				throw new WrongParametersException(sprintf("Job '%d' does not belong to writer '%s'", $params['jobId'], $this->configuration->writerId));
			}

			if (isset($params['detail']) && $params['detail'] == 'csv') {
				$result = json_decode($job['result'], true);
				if (isset($result['csvFile']) && file_exists($result['csvFile'])) {

					$response = new StreamedResponse(function() use($result) {
						$linesCount = !empty($params['limit']) ? $params['limit'] : -1;
						$handle = fopen($result['csvFile'], 'r');
						if ($handle === false) {
							return false;
						}
						while (($buffer = fgets($handle)) !== false && $linesCount != 0) {
							print $buffer;
							$linesCount--;
						}
						fclose($handle);
						return true;
					});
					$response->headers->set('Content-Disposition', $response->headers->makeDisposition(ResponseHeaderBag::DISPOSITION_ATTACHMENT, $params['jobId'] . '.csv'));
					$response->headers->set('Content-Length', filesize($result['csvFile']));
					$response->headers->set('Content-Type', 'text/csv');
					$response->headers->set('Access-Control-Allow-Origin', '*');
					$response->send();
					exit();

				} else {
					throw new WrongParametersException("There is no csvFile for this job");
				}
			} else {
				$job = $this->sharedConfig->jobToApiResponse($job, $this->_s3Client);
				return array('job' => $job);
			}
		}
	}

	/**
	 * Cancel waiting jobs
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postCancelJobs($params)
	{
		$this->_init($params);
		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}

		$this->sharedConfig->cancelJobs($this->configuration->projectId, $this->configuration->writerId);
		return array();
	}

	/**
	 * Get Batch
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getBatch($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (empty($params['batchId'])) {
			throw new WrongParametersException("Parameter 'batchId' is missing");
		}

		return array('batch' => $this->sharedConfig->batchToApiResponse($params['batchId'], $this->_s3Client));
	}




	private function _createJob($params)
	{
		$jobId = $this->_storageApi->generateId();
		if (!isset($params['batchId'])) {
			$params['batchId'] = $jobId;
		}

		$params['queueId'] = sprintf('%s.%s.%s', $this->configuration->projectId, $this->configuration->writerId,
			isset($params['queue']) ? $params['queue'] : SharedConfig::PRIMARY_QUEUE);
		unset($params['queue']);

		$jobInfo = array(
			'id' => $jobId,
			'runId' => $this->_storageApi->getRunId(),
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'token' => $this->_storageApi->token,
			'tokenId' => $this->configuration->tokenInfo['id'],
			'tokenDesc' => $this->configuration->tokenInfo['description'],
			'tokenOwnerName' => $this->configuration->tokenInfo['owner']['name'],
			'initializedBy' => null,
			'createdTime' => null,
			'startTime' => null,
			'endTime' => null,
			'backendUrl' => null,
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
			'log' => null,
			'projectIdWriterId' => sprintf('%s.%s', $this->configuration->projectId, $this->configuration->writerId)
		);
		$jobInfo = array_merge($jobInfo, $params);

		$this->sharedConfig->saveJob($jobId, $jobInfo);

		$message = "Writer job $jobId created manually";
		$results = array('jobId' => $jobId);
		$this->sharedConfig->logEvent($this->configuration->writerId, $jobInfo['runId'], $message, $params, $results);

		$this->_log->log(Logger::INFO, $message, array(
			'token' => $this->_storageApi->getLogData(),
			'configurationId' => $this->configuration->writerId,
			'runId' => $jobInfo['runId'],
			'params' => $params,
			'results' => $results
		));

		return $jobInfo;
	}

	/**
	 * @param $batchId
	 */
	protected function _enqueue($batchId)
	{
		$this->_queue->enqueue(array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'batchId' => $batchId
		));
	}


	protected function _waitForJob($jobId, $writerId)
	{
		$jobFinished = false;
		$i = 1;
		do {
			$jobInfo = $this->getJobs(array('jobId' => $jobId, 'writerId' => $writerId));
			if (isset($jobInfo['job']['status']) && !in_array($jobInfo['job']['status'], array('waiting', 'processing'))) {
				$jobFinished = true;
			}
			if (!$jobFinished) sleep($i * 10);
			$i++;
		} while(!$jobFinished);

		if ($jobInfo['job']['status'] == 'success') {
			return $jobInfo;
		} else {
			$e = new JobProcessException('Job processing failed');
			$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
			throw $e;
		}
	}

	protected function _waitForBatch($batchId, $writerId)
	{
		$jobsFinished = false;
		$i = 1;
		do {
			$jobsInfo = $this->getBatch(array('batchId' => $batchId, 'writerId' => $writerId));
			if (isset($jobsInfo['batch']['status']) && !in_array($jobsInfo['batch']['status'], array('waiting', 'processing'))) {
				$jobsFinished = true;
			}
			if (!$jobsFinished) sleep($i * 10);
			$i++;
		} while(!$jobsFinished);

		if ($jobsInfo['batch']['status'] == 'success') {
			return $jobsInfo;
		} else {
			$e = new JobProcessException('Batch processing failed');
			$e->setData(array('result' => $jobsInfo['batch']['result'], 'log' => $jobsInfo['batch']['log']));
			throw $e;
		}
	}

}
