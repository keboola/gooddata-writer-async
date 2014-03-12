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
use Keboola\GoodDataWriter\Exception\GraphTtlException;
use Keboola\GoodDataWriter\GoodData\CLToolApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\SSO,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedConfig;
use Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\Model\Graph;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Syrup\ComponentBundle\Component\Component;
use Monolog\Logger;
use Symfony\Component\HttpFoundation\Response;
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
	 * @var AppConfiguration
	 */
	private $appConfiguration;
	/**
	 * @var Service\S3Client
	 */
	private $s3Client;
	/**
	 * @var Service\Queue
	 */
	private $queue;

	/**
	 * Init Writer
	 */
	private function init($params)
	{
		if (!defined('JSON_PRETTY_PRINT')) {
			// fallback for PHP <= 5.3
			define('JSON_PRETTY_PRINT', 0);
		}
		/*StorageApiClient::setLogger(function($message, $data) {
			echo $message . PHP_EOL . PHP_EOL;
		});*/

		// Init params
		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Parameter \'writerId\' is missing');
		}

		if (isset($params['queue']) && !in_array($params['queue'], array(SharedConfig::PRIMARY_QUEUE, SharedConfig::SECONDARY_QUEUE))) {
			throw new WrongParametersException('Wrong parameter \'queue\'. Must be one of: ' . SharedConfig::PRIMARY_QUEUE . ', ' . SharedConfig::SECONDARY_QUEUE);
		}

		// Init main temp directory
		if (!$this->appConfiguration) {
			$this->appConfiguration = $this->_container->get('gooddata_writer.app_configuration');
		}
		$this->configuration = new Configuration($this->_storageApi, $params['writerId'], $this->appConfiguration->scriptsPath);
	}



	/**
	 * List all configured writers
	 */
	public function getWriters($params)
	{
		if (isset($params['writerId'])) {
			$this->init($params);
			if (!$this->configuration->bucketId) {
				throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
			}

			$result = array(
				'writer' => $this->configuration->formatWriterAttributes($this->configuration->bucketId, $this->configuration->bucketAttributes())
			);

			return $result;
		} else {
			if (!$this->appConfiguration) {
				$this->appConfiguration = $this->_container->get('gooddata_writer.app_configuration');
			}
			$this->configuration = new Configuration($this->_storageApi, null, $this->appConfiguration->scriptsPath);
			return array('writers' => $this->configuration->getWriters());
		}
	}


	/**
	 * Create new writer with main GoodData project and user
	 */
	public function postWriters($params)
	{
		$command = 'createWriter';
		$createdTime = time();


		$this->checkParams($params, array('writerId'));
		if (!preg_match('/^[a-zA-Z0-9_]+$/', $params['writerId'])) {
			throw new WrongParametersException('Parameter writerId may contain only basic letters, numbers and underscores');
		}
		$this->init($params);


		$this->configuration->createWriter($params['writerId'], isset($params['backendUrl']) ? $params['backendUrl'] : null);

		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->appConfiguration->gd_accessToken;
		$projectName = sprintf($this->appConfiguration->gd_projectNameTemplate, $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);


		$batchId = $this->_storageApi->generateId();
		$jobInfo = $this->createJob(array(
			'batchId' => $batchId,
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName
			)
		));

		if(empty($params['users'])) {
			$this->enqueue($batchId);

			if (empty($params['wait'])) {
				return array('job' => (int)$jobInfo['id']);
			} else {
				$result = $this->waitForJob($jobInfo['id'], $params['writerId']);
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
				$this->createJob(array(
					'batchId' => $batchId,
					'command' => 'addUserToProject',
					'createdTime' => date('c', $createdTime),
					'parameters' => array(
						'email' => $user,
						'role' => 'admin'
					)
				));
			}

			$this->enqueue($batchId);

			return array('batch' => (int)$batchId);
		}


	}


	/**
	 * Delete writer with projects and users
	 */
	public function deleteWriters($params)
	{
		$command = 'deleteWriter';
		$createdTime = time();

		$this->init($params);
		$this->checkWriterExistence($params);

		$this->configuration->updateWriter('toDelete', '1');

		$this->getSharedConfig()->cancelJobs($this->configuration->projectId, $this->configuration->writerId);

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array()
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			return $this->waitForJob($jobInfo['id'], $params['writerId']);
		}
	}


	/***********************
	 * @section Projects
	 */


	/**
	 * List projects from configuration
	 */
	public function getProjects($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);

		return array('projects' => $this->configuration->getProjects());
	}


	/**
	 * Clone project
	 */
	public function postProjects($params)
	{
		$command = 'cloneProject';
		$createdTime = time();

		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->appConfiguration->gd_accessToken;
		$projectName = !empty($params['name']) ? $params['name']
			: sprintf($this->appConfiguration->gd_projectNameTemplate, $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();


		$bucketAttributes = $this->configuration->bucketAttributes();
		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'includeData' => empty($params['includeData']) ? 0 : 1,
				'includeUsers' => empty($params['includeUsers']) ? 0 : 1,
				'pidSource' => $bucketAttributes['gd']['pid']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);
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
	 */
	public function getProjectUsers($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('pid'));

		return array('users' => $this->configuration->getProjectUsers($params['pid']));
	}


	/**
	 * Add User to Project
	 */
	public function postProjectUsers($params)
	{
		$command = 'addUserToProject';
		$createdTime = time();


		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('email', 'pid', 'role'));
		$allowedRoles = array_keys(RestApi::$userRoles);
		if (!in_array($params['role'], $allowedRoles)) {
			throw new WrongParametersException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', $allowedRoles));
		}
		if (!$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();
		$this->configuration->checkUsersTable();
		$this->configuration->checkProjectUsersTable();


		$jobInfo = $this->createJob(array(
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
		$this->enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			return $this->waitForJob($jobInfo['id'], $params['writerId']);
		}
	}


	/**
	 * Remove User from Project
	 */
	public function deleteProjectUsers($params)
	{
		$command = 'removeUserFromProject';
		$createdTime = time();

		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('email', 'pid'));
		if (!$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		if (!$this->configuration->isProjectUser($params['email'], $params['pid'])) {
			throw new WrongParametersException(sprintf("Project user '%s' is not configured for the writer", $params['email']));
		}
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();
		$this->configuration->checkProjectUsersTable();

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params,
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

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
	 */
	public function getUsers($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);

		if (isset($params['userEmail'])) {
			$user = $this->configuration->getUser($params['userEmail']);
			return array('user' => $user ? $user : null);
		} else {
			return array('users' => $this->configuration->getUsers());
		}
	}

	/**
	 * Create user
	 */
	public function postUsers($params)
	{
		$command = 'createUser';
		$createdTime = time();


		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('firstName', 'lastName', 'email', 'password'));
		if (strlen($params['password']) < 7) {
			throw new WrongParametersException("Parameter 'password' must have at least seven characters");
		}
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkUsersTable();


		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'firstName' => $params['firstName'],
				'lastName' => $params['lastName'],
				'email' => $params['email'],
				'password' => $params['password'],
				'ssoProvider' => empty($params['ssoProvider']) ? null : $params['ssoProvider']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['uid'])) {
				return array('uid' => $result['job']['result']['uid']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	/**
	 * Generate SSO link
	 */
	public function getSso($params)
	{
		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('email', 'pid'));
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

		$sso = new SSO($this->configuration, $this->appConfiguration);

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



	/***********************
	 * @section Proxy
	 */

	/**
	 * Call GD Api with GET request
	 */
	public function getProxy($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('query'));

		// query validation
		try {
			// clean url - remove domain
			$query = Url::factory(urldecode($params['query']));

			$url = Url::buildUrl(array(
				'path' => $query->getPath(),
				'query' => $query->getQuery(),
			));
		} catch (InvalidArgumentException $e) {
			throw new WrongParametersException("Wrong value for 'query' parameter given");
		}

		/**
		 * @var RestApi
		 */
		$restApi = $this->_container->get('gooddata_writer.rest_api');

		$bucketAttributes = $this->configuration->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		try {
			$return = $restApi->get($url);

			return array(
				'response' => $return
			);
		} catch (RestApiException $e) {
			throw new WrongParametersException($e->getMessage(), $e);
		}
	}

	/**
	 * Call GD Api with POST request
	 */
	public function postProxy($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('query', 'payload'));

		$jobInfo = $this->createJob(array(
			'command'       => 'proxyCall',
			'createdTime'   => date('c', time()),
			'parameters'    => array(
				'query'     => $params['query'],
				'payload'   => $params['payload']
			),
			'queue'         => isset($params['queue']) ? $params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);

		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);

			if (isset($result['job']['status']) && $result['job']['status'] == 'success') {
				return array(
					'message'   => 'proxy call executed',
					'response'  => $result['job']['result']['response']
				);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	/***********************
	 * @section Filters
	 */


	/**
	 * Returns list of filters configured in writer
	 * If 'userEmail' parameter is specified, only returns filters for specified user
	 */
	public function getFilters($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);

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
	 */
	public function postFilters($params)
	{
		$command = 'createFilter';
		$createdTime = time();

		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('name', 'attribute', 'element', 'pid'));
		if (!isset($params['operator'])) {
			$params['operator'] = '=';
		}

		$attr = explode('.', $params['attribute']);
		if (count($attr) != 4) {
			throw new WrongParametersException("Parameter 'attribute' should contain identifier of column in Storage API, e.g. out.c-main.table.column");
		}
		$tableId = sprintf('%s.%s.%s', $attr[0], $attr[1], $attr[2]);
		$sapiTable = $this->configuration->getSapiTable($tableId);
		if (!in_array($attr[3], $sapiTable['columns'])) {
			throw new WrongParametersException(sprintf("Column '%s' of parameter 'attribute' does not exist in table '%s'", $attr[3], $tableId));
		}

		$jobInfo = $this->createJob(array(
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
		$this->enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);

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
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('uri'));

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'uri' => $params['uri']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);
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
	 */
	public function postFiltersUser($params)
	{
		$command = 'assignFiltersToUser';
		$createdTime = time();

		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('filters', 'userEmail', 'pid'));

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'filters' => $params['filters'],
				'userEmail' => $params['userEmail'],
				'pid' => $params['pid']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);
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
	 */
	public function postSyncFilters($params)
	{
		$command = 'syncFilters';
		$createdTime = time();

		$this->init($params);
		$this->checkWriterExistence($params);

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'pid' => isset($params['pid']) ? $params['pid'] : null
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);

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
	 * Generates LDM model of writer
	 */
	public function getLdm($params)
	{
		$this->init($params);

		$this->checkWriterExistence($params);

		//@TODO return $this->configuration->getLDM();
	}

	/**
	 *
	 */
	public function getXml($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('tableId'));

		$response = new Response(CLToolApi::getXml($params['tableId']));
		$response->headers->set('Content-Type', 'application/xml');
		$response->headers->set('Access-Control-Allow-Origin', '*');
		$response->send();
		exit();
	}

	/**
	 * Upload configured date dimension to GoodData
	 */
	public function postUploadDateDimension($params)
	{
		$createdTime = time();

		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('tableId', 'name'));

		$this->configuration->checkBucketAttributes();
		$dateDimensions = $this->configuration->getDateDimensions();
		if (!in_array($params['name'], array_keys($dateDimensions))) {
			throw new WrongParametersException(sprintf("Date dimension '%s' does not exist in configuration", $params['name']));
		}

		$jobInfo = $this->createJob(array(
			'command' => 'UploadDateDimension',
			'createdTime' => date('c', $createdTime),
			'dataset' => $params['name'],
			'parameters' => array(
				'name' => $params['name'],
				'includeTime' => $dateDimensions[$params['name']]['includeTime']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));
		if (isset($params['pid'])) {
			$jobData['parameters']['pid'] = $params['pid'];
		}

		$this->enqueue($jobInfo['batchId']);
		return array('job' => (int)$jobInfo['id']);
	}

	public function postUploadTable($params)
	{
		$createdTime = time();

		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('tableId'));

		$this->configuration->checkBucketAttributes();

		$batchId = $this->_storageApi->generateId();

		$definition = $this->configuration->getDataSetDefinition($params['tableId']);


		// Create date dimensions
		$dateDimensionsToLoad = array();
		$dateDimensions = array();
		if ($definition['columns']) foreach ($definition['columns'] as $column) if ($column['type'] == 'DATE') {
			if (!$dateDimensions) {
				$dateDimensions = $this->configuration->getDateDimensions();
			}

			$dimension = $column['schemaReference'];
			if (!isset($dateDimensions[$dimension])) {
				throw new WrongParametersException(sprintf("Date dimension '%s' defined for column '%s' does not exist in configuration", $dimension, $column['name']));
			}

			if (!$dateDimensions[$dimension]['isExported'] && !in_array($dimension, $dateDimensionsToLoad)) {
				$dateDimensionsToLoad[] = $dimension;

				$jobData = array(
					'batchId' => $batchId,
					'command' => 'uploadDateDimension',
					'dataset' => $dimension,
					'createdTime' => date('c', $createdTime),
					'parameters' => array(
						'name' => $dimension,
						'includeTime' => $dateDimensions[$dimension]['includeTime']
					),
					'queue' => isset($params['queue']) ? $params['queue'] : null
				);
				if (isset($params['pid'])) {
					$jobData['parameters']['pid'] = $params['pid'];
				}
				$this->createJob($jobData);
			}
		}

		$tableConfiguration = $this->configuration->getDataSet($params['tableId']);
		$jobData = array(
			'batchId' => $batchId,
			'command' => 'uploadTable',
			'dataset' => !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'],
			'createdTime' => date('c', $createdTime),
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
		$jobInfo = $this->createJob($jobData);


		$this->sharedConfig->saveJob($jobInfo['id'], array(
			'definition' => $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $params['tableId']), json_encode($definition))
		));

		$this->enqueue($batchId);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			return $this->waitForJob($jobInfo['id'], $params['writerId']);
		}
	}

	/**
	 * Upload whole project
	 */
	public function postUploadProject($params)
	{
		$createdTime = time();

		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->configuration->checkBucketAttributes();

		$this->configuration->getDateDimensions();
		$runId = $this->_storageApi->getRunId();
		$batchId = $this->_storageApi->generateId();

		$sortedDataSets = $this->configuration->getSortedDataSets();


		// Create date dimensions
		$dateDimensionsToLoad = array();
		$dateDimensions = array();
		foreach ($sortedDataSets as $dataSet) {
			if ($dataSet['definition']['columns']) foreach ($dataSet['definition']['columns'] as $column) if ($column['type'] == 'DATE') {
				if (!$dateDimensions) {
					$dateDimensions = $this->configuration->getDateDimensions();
				}

				$dimension = $column['schemaReference'];
				if (!isset($dateDimensions[$dimension])) {
					throw new WrongParametersException(sprintf("Date dimension '%s' defined for table '%s' and column '%s' does not exist in configuration", $dimension, $dataSet['tableId'], $column['name']));
				}

				if (!$dateDimensions[$dimension]['isExported'] && !in_array($dimension, $dateDimensionsToLoad)) {
					$dateDimensionsToLoad[] = $dimension;

					$jobData = array(
						'batchId' => $batchId,
						'command' => 'uploadDateDimension',
						'dataset' => $dimension,
						'createdTime' => date('c', $createdTime),
						'parameters' => array(
							'name' => $dimension,
							'includeTime' => $dateDimensions[$dimension]['includeTime']
						),
						'queue' => isset($params['queue']) ? $params['queue'] : null
					);
					if (isset($params['pid'])) {
						$jobData['parameters']['pid'] = $params['pid'];
					}
					$this->createJob($jobData);
				}
			}
		}

		foreach ($sortedDataSets as $dataSet) {
			$jobData = array(
				'batchId' => $batchId,
				'runId' => $runId,
				'command' => 'uploadTable',
				'dataset' => $dataSet['title'],
				'createdTime' => date('c', $createdTime),
				'parameters' => array(
					'tableId' => $dataSet['tableId']
				),
				'queue' => isset($params['queue']) ? $params['queue'] : null
			);
			if (isset($params['pid'])) {
				$jobData['parameters']['pid'] = $params['pid'];
			}
			if (isset($params['incrementalLoad'])) {
				$jobData['parameters']['incrementalLoad'] = $params['incrementalLoad'];
			}
			$jobInfo = $this->createJob($jobData);

			$this->sharedConfig->saveJob($jobInfo['id'], array(
				'definition' => $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $dataSet['tableId']), json_encode($dataSet['definition']))
			));
		}

		// Execute reports
		$jobData = array(
			'batchId' => $batchId,
			'runId' => $runId,
			'command' => 'executeReports',
			'createdTime' => date('c', $createdTime),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		);
		$this->createJob($jobData);

		$this->enqueue($batchId);


		if (empty($params['wait'])) {
			return array('batch' => (int)$batchId);
		} else {
			return $this->waitForBatch($batchId, $params['writerId']);
		}
	}

	/**
	 * Execute Reports
	 */
	public function postExecuteReports($params)
	{
		$command = 'executeReports';
		$createdTime = time();

		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('pid'));
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();

		$project = $this->configuration->getProject($params['pid']);
		if (!$project) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}

		if (!$project['active']) {
			throw new WrongParametersException(sprintf("Project '%s' is not active", $params['pid']));
		}

		$reports = array();
		if (!empty($params['reports'])) {
			$reports = (array) $params['reports'];

			foreach ($reports AS $reportLink) {
				if (!preg_match('/^\/gdc\/md\/' . $params['pid'] . '\//', $reportLink)) {
					throw new WrongParametersException("Parameter 'reports' is not valid; report uri '" .$reportLink . "' does not belong to the project");
				}
			}
		}


		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'pid' => $params['pid'],
				'reports' => $reports,
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['status']) && $result['job']['status'] == 'success') {
				return array('message' => 'Reports executed');
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}

	}

	public function postExportReport($params)
	{
		$command = 'exportReport';
		$createdTime = time();

		// Init parameters
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('pid', 'report', 'table'));

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'pid'       => $params['pid'],
				'report'    => $params['report'],
				'table'     => $params['table']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['status']) && $result['job']['status'] == 'success') {
				return array('message' => 'Report exported to SAPI');
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
	 */
	public function getModel($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);

		$model = new Graph();
		$dimensionsUrl = sprintf('%s/admin/projects-new/%s/gooddata?config=%s#/date-dimensions',
			$this->_container->getParameter('storage_api.url'), $this->configuration->projectId, $this->configuration->writerId);
		$tableUrl = sprintf('%s/admin/projects-new/%s/gooddata?config=%s#/table/',
			$this->_container->getParameter('storage_api.url'), $this->configuration->projectId, $this->configuration->writerId);
		$model->setTableUrl($tableUrl);
		$model->setDimensionsUrl($dimensionsUrl);

		try {
			$result = $model->getGraph($this->configuration);
		} catch(GraphTtlException $e) {
			throw new HttpException(400, "Model too large.", $e);
		}

		$response = new Response(json_encode($result));
		$response->headers->set('Content-Type', 'application/json');
		$response->headers->set('Access-Control-Allow-Origin', '*');
		$response->send();
		exit();
	}

	/**
	 * Get tables configuration
	 */
	public function getTables($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);

		if (isset($params['tableId'])) {
			// Table detail
			return array('table' => $this->configuration->getDataSetForApi($params['tableId']));
		} elseif (isset($params['referenceable'])) {
			return array('tables' => $this->configuration->getDataSetsWithConnectionPointOld());
		} elseif (isset($params['connection'])) {
			return array('tables' => $this->configuration->getDataSetsWithConnectionPoint());
		} else {
			return array('tables' => $this->configuration->getDataSets());
		}
	}

	/**
	 * Update tables configuration
	 */
	public function postTables($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('tableId'));
		if (!in_array($params['tableId'], $this->configuration->getOutputSapiTables())) {
			throw new WrongParametersException(sprintf("Table '%s' does not exist", $params['tableId']));
		}

		$tableId = $params['tableId'];
		unset($params['tableId']);

		$this->configuration->updateDataSetsFromSapi();

		if (isset($params['column'])) {
			$columnName = trim($params['column']);
			unset($params['column']);

			// Column detail
			$this->configuration->updateColumnsDefinition($tableId, $columnName, $params);

		} elseif (isset($params['columns'])) {
			$this->configuration->updateColumnsDefinition($tableId, $params['columns']);
		} else {
			// Table detail
			$this->configuration->updateDataSetDefinition($tableId, $params);
		}

		return array();
	}


	/**
	 * Reset export status of all dataSets and dimensions
	 */
	public function postResetExport($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);

		foreach ($this->configuration->getDataSets() as $dataSet) if (!empty($dataSet['isExported'])) {
			$this->configuration->updateDataSetDefinition($dataSet['id'], 'isExported', 0);
		}

		return array();
	}

	/**
	 * Reset dataSet and remove it from GoodData project
	 */
	public function postResetTable($params)
	{
		$command = 'resetTable';
		$createdTime = time();

		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('tableId'));

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'tableId' => $params['tableId']
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		return array('job' => (int)$jobInfo['id']);
	}


	/**
	 * Reset GoodData project
	 */
	public function postResetProject($params)
	{
		$command = 'resetProject';
		$createdTime = time();

		$this->init($params);
		$this->checkWriterExistence($params);

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'removeClones' => isset($params['removeClones'])? (bool)$params['removeClones'] : false
			),
			'queue' => isset($params['queue']) ? $params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		return array('job' => (int)$jobInfo['id']);
	}


	/**
	 * Get all configured date dimensions
	 */
	public function getDateDimensions($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);

		return array('dimensions' => (object) $this->configuration->getDateDimensions(isset($params['usage'])));
	}


	/**
	 * Delete configured date dimension
	 */
	public function deleteDateDimensions($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('name'));

		$dimensions = $this->configuration->getDateDimensions();
		if (isset($dimensions[$params['name']])) {
			$this->configuration->deleteDateDimension($params['name']);
			return array();
		} else {
			throw new WrongParametersException(sprintf("Dimension '%s' does not exist", $params['name']));
		}
	}


	/**
	 * Update configured date dimension or create new
	 */
	public function postDateDimensions($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('name'));

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
	 * Allow filtering by days, command and tableId
	 */
	public function getJobs($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);

		if (empty($params['jobId'])) {
			$days = isset($params['days']) ? $params['days'] : 7;
			$tableId = empty($params['tableId']) ? null : $params['tableId'];
			$command = empty($params['command']) ? null : $params['command'];
			$tokenId = empty($params['tokenId']) ? null : $params['tokenId'];
			$status = empty($params['status']) ? null : $params['status'];
			$jobs = $this->getSharedConfig()->fetchJobs($this->configuration->projectId, $params['writerId'], $days, $tableId);

			$result = array();
			foreach ($jobs as $job) {
				if ((empty($command) || $command == $job['command']) && (empty($tokenId) || $tokenId == $job['tokenId'])
					&& (empty($status) || $status == $job['status'])) {
					$job = $this->getSharedConfig()->jobToApiResponse($job, $this->getS3Client());
					if (empty($tableId) || (!empty($job['parameters']['tableId']) && $job['parameters']['tableId'] == $tableId)) {
						$result[] = $job;
					}
				}
			}

			return array('jobs' => $result);
		} else {
			if (is_array($params['jobId'])) {
				throw new WrongParametersException("Parameter 'jobId' has to be a number");
			}
			$job = $this->getSharedConfig()->fetchJob($params['jobId'], $this->configuration->writerId, $this->configuration->projectId);
			if (!$job) {
				throw new WrongParametersException(sprintf("Job '%d' does not belong to writer '%s'", $params['jobId'], $this->configuration->writerId));
			}

			$job = $this->getSharedConfig()->jobToApiResponse($job, $this->getS3Client());
			return array('job' => $job);
		}
	}

	/**
	 * Cancel waiting jobs
	 */
	public function postCancelJobs($params)
	{
		$this->init($params);
		$this->checkParams($params, array('writerId'));

		$this->getSharedConfig()->cancelJobs($this->configuration->projectId, $this->configuration->writerId);
		return array();
	}

	public function postMigrateConfiguration($params)
	{
		$this->init($params);
		$this->configuration->migrateConfiguration();
	}

	/**
	 * Get Batch
	 */
	public function getBatch($params)
	{
		$this->init($params);
		$this->checkWriterExistence($params);
		$this->checkParams($params, array('batchId'));

		return array('batch' => $this->getSharedConfig()->batchToApiResponse($params['batchId'], $this->getS3Client()));
	}




	/***************************************************************************
	 * *************************************************************************
	 * @section Helpers
	 */

	private function getS3Client()
	{
		if (!$this->s3Client) {
			if (!$this->appConfiguration) {
				$this->appConfiguration = $this->_container->get('gooddata_writer.app_configuration');
			}
			$this->s3Client = new Service\S3Client(
				$this->appConfiguration,
				$this->configuration->projectId . '.' . $this->configuration->writerId
			);
		}
		return $this->s3Client;
	}

	private function getWriterQueue()
	{
		if (!$this->queue) {
			if (!$this->appConfiguration) {
				$this->appConfiguration = $this->_container->get('gooddata_writer.app_configuration');
			}
			$this->queue = $this->_container->get('gooddata_writer.jobs_queue');
		}
		return $this->queue;
	}

	private function getSharedConfig()
	{
		if (!$this->sharedConfig) {
			if (!$this->appConfiguration) {
				$this->appConfiguration = $this->_container->get('gooddata_writer.app_configuration');
			}
			$this->sharedConfig = $this->_container->get('gooddata_writer.shared_config');
		}
		return $this->sharedConfig;
	}

	private function checkParams($params, $required)
	{
		foreach($required as $k) {
			if (empty($params[$k])) {
				throw new WrongParametersException("Parameter '" . $k . "' is missing");
			}
		}
	}

	private function checkWriterExistence($params)
	{
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
	}

	private function createJob($params)
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
			'createdTime' => null,
			'startTime' => null,
			'gdWriteStartTime' => null,
			'endTime' => null,
			'command' => null,
			'dataset' => null,
			'parameters' => null,
			'result' => null,
			'status' => 'waiting',
			'logs' => null,
			'debug' => null,
			'projectIdWriterId' => sprintf('%s.%s', $this->configuration->projectId, $this->configuration->writerId)
		);
		$jobInfo = array_merge($jobInfo, $params);

		$this->getSharedConfig()->saveJob($jobId, $jobInfo);

		$this->_log->log(Logger::INFO, 'Job created ' . $jobId, array(
			'writerId' => $this->configuration->writerId,
			'runId' => $jobInfo['runId'],
			'params' => $params
		));

		return $jobInfo;
	}

	/**
	 * @param $batchId
	 */
	protected function enqueue($batchId)
	{
		$this->getWriterQueue()->enqueue(array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'batchId' => $batchId
		));
	}


	protected function waitForJob($jobId, $writerId)
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

	protected function waitForBatch($batchId, $writerId)
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
