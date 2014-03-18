<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-03-14
 */

namespace Keboola\GoodDataWriter\Controller;


use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Method;

use Guzzle\Http\Url,
	Guzzle\Common\Exception\InvalidArgumentException;
use Monolog\Logger;
use Symfony\Component\HttpFoundation\Response,
	Symfony\Component\HttpKernel\Exception\HttpException,
	Symfony\Component\Stopwatch\Stopwatch;

use Syrup\ComponentBundle\Exception\SyrupComponentException;
use Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\SSO,
	Keboola\GoodDataWriter\Model\Graph,
	Keboola\GoodDataWriter\Service\S3Client,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedConfig,
	Keboola\GoodDataWriter\Writer\AppConfiguration;
use Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\Exception\GraphTtlException,
	Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\Exception\WrongParametersException;


class ApiController extends \Syrup\ComponentBundle\Controller\ApiController
{

	/**
	 * @var Configuration
	 */
	public $configuration;
	/**
	 * @var SharedConfig
	 */
	public $sharedConfig;

	/**
	 * @var AppConfiguration
	 */
	private $appConfiguration;
	/**
	 * @var S3Client
	 */
	private $s3Client;
	/**
	 * @var \Keboola\GoodDataWriter\Service\Queue
	 */
	private $queue;

	private $method;
	private $params;

	/**
	 * @var StopWatch
	 */
	private $stopWatch;


	/**
	 * Common things to do for each request
	 */
	public function preExecute()
	{
		parent::preExecute();
		
		if (!defined('JSON_PRETTY_PRINT')) {
			// fallback for PHP <= 5.3
			define('JSON_PRETTY_PRINT', 0);
		}
		set_time_limit(3 * 60 * 60);

		// Get params
		$request = $this->getRequest();
		$this->method = $request->getMethod();
		$this->params = in_array($this->method, array('POST', 'PUT'))? $this->getPostJson($request) : $request->query->all();

		if (isset($this->params['queue']) && !in_array($this->params['queue'], array(SharedConfig::PRIMARY_QUEUE, SharedConfig::SECONDARY_QUEUE))) {
			throw new WrongParametersException('Wrong parameter \'queue\'. Must be one of: ' . SharedConfig::PRIMARY_QUEUE . ', ' . SharedConfig::SECONDARY_QUEUE);
		}

		$this->appConfiguration = $this->container->get('gooddata_writer.app_configuration');
		$this->stopWatch = new Stopwatch();
		$this->stopWatch->start('request');
	}



	/**
	 * Create writer
	 *
	 * @Route("/writers")
	 * @Method({"POST"})
	 */
	public function postWritersAction()
	{
		$command = 'createWriter';
		$createdTime = time();

		$this->checkParams(array('writerId'));
		if (!preg_match('/^[a-zA-Z0-9_]+$/', $this->params['writerId'])) {
			throw new WrongParametersException('Parameter writerId may contain only basic letters, numbers and underscores');
		}

		$this->getConfiguration()->createWriter($this->params['writerId'], isset($this->params['backendUrl']) ? $this->params['backendUrl'] : null);

		$accessToken = !empty($this->params['accessToken']) ? $this->params['accessToken'] : $this->appConfiguration->gd_accessToken;
		$projectName = sprintf($this->appConfiguration->gd_projectNameTemplate, $this->getConfiguration()->tokenInfo['owner']['name'], $this->getConfiguration()->writerId);


		$batchId = $this->storageApi->generateId();
		$jobInfo = $this->createJob(array(
			'batchId' => $batchId,
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName
			)
		));

		if(empty($this->params['users'])) {
			$this->enqueue($batchId);

			if (empty($this->params['wait'])) {
				return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
			} else {
				$result = $this->waitForJob($jobInfo['id'], false);
				if (isset($result['job']['result']['pid'])) {
					return $this->createApiResponse(array('pid' => $result['job']['result']['pid']));
				} else {
					$e = new JobProcessException('Job failed');
					$e->setData(array('result' => $result['job']['result'], 'logs' => $result['job']['logs']));
					throw $e;
				}
			}
		} else {

			$users = explode(',', $this->params['users']);
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

			return $this->getPollResult($batchId, $this->params['writerId'], true);
		}
	}

	/**
	 * Delete writer
	 *
	 * @Route("/writers")
	 * @Method({"DELETE"})
	 */
	public function deleteWritersAction()
	{
		$command = 'deleteWriter';
		$createdTime = time();

		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		$this->getConfiguration()->updateWriter('toDelete', '1');

		$this->getSharedConfig()->cancelJobs($this->getConfiguration()->projectId, $this->getConfiguration()->writerId);

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array()
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			return $this->waitForJob($jobInfo['id']);
		}
	}

	/**
	 * Detail writer or list all writers
	 *
	 * @Route("/writers")
	 * @Method({"GET"})
	 */
	public function getWritersAction()
	{
		if (isset($this->params['writerId'])) {
			$this->checkWriterExistence();

			return $this->createApiResponse(array(
				'writer' => $this->getConfiguration()->formatWriterAttributes($this->getConfiguration()->bucketId, $this->getConfiguration()->bucketAttributes())
			));
		} else {
			$configuration = new Configuration($this->storageApi, null, $this->appConfiguration->scriptsPath);
			return $this->createApiResponse(array(
				'writers' => $configuration->getWriters()
			));
		}
	}



	/**
	 * Create projects by cloning
	 *
	 * @Route("/projects")
	 * @Method({"POST"})
	 */
	public function postProjectsAction()
	{
		$command = 'cloneProject';
		$createdTime = time();

		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();
		$this->getConfiguration()->checkBucketAttributes();
		$this->getConfiguration()->checkProjectsTable();

		$accessToken = !empty($this->params['accessToken']) ? $this->params['accessToken'] : $this->appConfiguration->gd_accessToken;
		$projectName = !empty($this->params['name']) ? $this->params['name']
			: sprintf($this->appConfiguration->gd_projectNameTemplate, $this->getConfiguration()->tokenInfo['owner']['name'], $this->getConfiguration()->writerId);


		$bucketAttributes = $this->getConfiguration()->bucketAttributes();
		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'includeData' => empty($this->params['includeData']) ? 0 : 1,
				'includeUsers' => empty($this->params['includeUsers']) ? 0 : 1,
				'pidSource' => $bucketAttributes['gd']['pid']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], false);
			if (isset($result['job']['result']['pid'])) {
				return $this->createApiResponse(array(
					'pid' => $result['job']['result']['pid']
				));
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'logs' => $result['job']['logs']));
				throw $e;
			}
		}
	}

	/**
	 * List cloned projects
	 *
	 * @Route("/projects")
	 * @Method({"GET"})
	 */
	public function getProjectsAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		return $this->createApiResponse(array(
			'projects' => $this->getConfiguration()->getProjects()
		));
	}


	/**
	 * Create project users
	 *
	 * @Route("/project-users")
	 * @Method({"POST"})
	 */
	public function postProjectUsersAction()
	{
		$command = 'addUserToProject';
		$createdTime = time();

		$this->checkParams(array('writerId', 'pid', 'email', 'role'));
		$this->checkWriterExistence();

		$allowedRoles = array_keys(RestApi::$userRoles);
		if (!in_array($this->params['role'], $allowedRoles)) {
			throw new WrongParametersException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', $allowedRoles));
		}
		if (!$this->getConfiguration()->getProject($this->params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $this->params['pid']));
		}
		$this->getConfiguration()->checkBucketAttributes();
		$this->getConfiguration()->checkProjectsTable();
		$this->getConfiguration()->checkUsersTable();
		$this->getConfiguration()->checkProjectUsersTable();


		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'email' => $this->params['email'],
				'pid' => $this->params['pid'],
				'role' => $this->params['role'],
				'createUser' => isset($this->params['createUser']) ? 1 : null
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			return $this->waitForJob($jobInfo['id']);
		}
	}

	/**
	 * Delete project users
	 *
	 * @Route("/project-users")
	 * @Method({"DELETE"})
	 */
	public function deleteProjectUsersAction()
	{
		$command = 'removeUserFromProject';
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId', 'pid', 'email'));
		$this->checkWriterExistence();
		if (!$this->getConfiguration()->getProject($this->params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $this->params['pid']));
		}
		if (!$this->getConfiguration()->isProjectUser($this->params['email'], $this->params['pid'])) {
			throw new WrongParametersException(sprintf("Project user '%s' is not configured for the writer", $this->params['email']));
		}
		$this->getConfiguration()->checkBucketAttributes();
		$this->getConfiguration()->checkProjectsTable();
		$this->getConfiguration()->checkProjectUsersTable();

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $this->params,
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			$jobId = $jobInfo['id'];
			$jobFinished = false;
			do {
				$job = $this->getSharedConfig()->fetchJob($jobId, $this->getConfiguration()->writerId, $this->getConfiguration()->projectId);
				if (!$job) {
					throw new WrongParametersException(sprintf("Job '%d' not found", $this->params['jobId']));
				}
				$jobInfo = $this->getSharedConfig()->jobToApiResponse($job, $this->getS3Client());
				if (isset($jobInfo['status']) && ($jobInfo['status'] == 'success' || $jobInfo['status'] == 'error')) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep(30);
			} while(!$jobFinished);

			if ($jobInfo['status'] == 'success') {
				return $this->createApiResponse();
			} else {
				$e = new JobProcessException('Remove Project User job failed');
				$e->setData(array('result' => $jobInfo['result'], 'logs' => $jobInfo['logs']));
				throw $e;
			}
		}
	}

	/**
	 * List project users
	 *
	 * @Route("/project-users")
	 * @Method({"GET"})
	 */
	public function getProjectUsersAction()
	{
		$this->checkParams(array('writerId', 'pid'));
		$this->checkWriterExistence();

		return $this->createApiResponse(array(
			'users' => $this->getConfiguration()->getProjectUsers($this->params['pid'])
		));
	}


	/**
	 * Create users
	 *
	 * @Route("/users")
	 * @Method({"POST"})
	 */
	public function postUsersAction()
	{
		$command = 'createUser';
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId', 'firstName', 'lastName', 'email', 'password'));
		$this->checkWriterExistence();
		if (strlen($this->params['password']) < 7) {
			throw new WrongParametersException("Parameter 'password' must have at least seven characters");
		}
		$this->getConfiguration()->checkBucketAttributes();
		$this->getConfiguration()->checkUsersTable();


		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'firstName' => $this->params['firstName'],
				'lastName' => $this->params['lastName'],
				'email' => $this->params['email'],
				'password' => $this->params['password'],
				'ssoProvider' => empty($this->params['ssoProvider']) ? null : $this->params['ssoProvider']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], false);
			if (isset($result['job']['result']['uid'])) {
				return $this->createApiResponse(array(
					'uid' => $result['job']['result']['uid']
				));
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'logs' => $result['job']['logs']));
				throw $e;
			}
		}
	}

	/**
	 * List users
	 *
	 * @Route("/users")
	 * @Method({"GET"})
	 */
	public function getUsersAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		if (isset($this->params['userEmail'])) {
			$user = $this->getConfiguration()->getUser($this->params['userEmail']);
			return $this->createApiResponse(array(
				'user' => $user ? $user : null
			));
		} else {
			return $this->createApiResponse(array(
				'users' => $this->getConfiguration()->getUsers()
			));
		}
	}



	/**
	 * Generate SSO link
	 *
	 * @Route("/sso")
	 * @Method({"GET"})
	 */
	public function getSsoAction()
	{
		// Init parameters
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();
		$this->checkParams(array('email', 'pid'));
		if (!empty($this->params['pid']) && !$this->getConfiguration()->getProject($this->params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $this->params['pid']));
		}

		if (!empty($this->params['createUser']) && $this->params['createUser'] == 1) {
			$this->params['wait'] = 1;
			$this->postUsersAction();
			$this->postProjectUsersAction();
		}

		$user = $this->getConfiguration()->getUser($this->params['email']);
		if (!$user) {
			throw new WrongParametersException("User " . $user . " doesn't exist in writer");
		}

		$sso = new SSO($this->getConfiguration(), $this->appConfiguration);

		$targetUrl = '/#s=/gdc/projects/' . $this->params['pid'];
		if (isset($this->params['targetUrl'])) {
			$targetUrl = $this->params['targetUrl'];
		}

		$validity = (isset($this->params['validity']))?$this->params['validity']:86400;

		$ssoLink = $sso->url($targetUrl, $this->params['email'], $validity);

		if (null == $ssoLink) {
			throw new SyrupComponentException(500, "Can't generate SSO link. Something is broken.");
		}

		return $this->createApiResponse(array(
			'ssoLink' => $ssoLink
		));
	}



	/**
	 * Call GD Api with POST request
	 *
	 * @Route("/proxy")
	 * @Method({"POST"})
	 */
	public function postProxyAction()
	{
		$this->checkParams(array('writerId', 'query', 'payload'));
		$this->checkWriterExistence();

		$jobInfo = $this->createJob(array(
			'command'       => 'proxyCall',
			'createdTime'   => date('c', time()),
			'parameters'    => array(
				'query'     => $this->params['query'],
				'payload'   => $this->params['payload']
			),
			'queue'         => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);

		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], false);

			if (isset($result['job']['result']['response'])) {
				return $this->createApiResponse(array(
					'message'   => 'proxy call executed',
					'response'  => $result['job']['result']['response']
				));
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'logs' => $result['job']['logs']));
				throw $e;
			}
		}
	}

	/**
	 * Call GD Api with GET request
	 *
	 * @Route("/proxy")
	 * @Method({"GET"})
	 */
	public function getProxyAction()
	{
		$this->checkParams(array('writerId', 'query'));
		$this->checkWriterExistence();

		// query validation
		try {
			// clean url - remove domain
			$query = Url::factory(urldecode($this->params['query']));

			$url = Url::buildUrl(array(
				'path' => $query->getPath(),
				'query' => $query->getQuery(),
			));
		} catch (InvalidArgumentException $e) {
			throw new WrongParametersException("Wrong value for 'query' parameter given");
		}

		/** @var RestApi $restApi */
		$restApi = $this->container->get('gooddata_writer.rest_api');

		$bucketAttributes = $this->getConfiguration()->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		try {
			$return = $restApi->get($url);

			return $this->createApiResponse(array(
				'response' => $return
			));
		} catch (RestApiException $e) {
			throw new WrongParametersException($e->getMessage(), $e);
		}
	}



	/**
	 * Create new user filter
	 *
	 * @Route("/filters")
	 * @Method({"POST"})
	 */
	public function postFiltersAction()
	{
		$command = 'createFilter';
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId', 'name', 'attribute', 'element', 'pid'));
		$this->checkWriterExistence();
		if (!isset($this->params['operator'])) {
			$this->params['operator'] = '=';
		}

		$attr = explode('.', $this->params['attribute']);
		if (count($attr) != 4) {
			throw new WrongParametersException("Parameter 'attribute' should contain identifier of column in Storage API, e.g. out.c-main.table.column");
		}
		$tableId = sprintf('%s.%s.%s', $attr[0], $attr[1], $attr[2]);
		$sapiTable = $this->getConfiguration()->getSapiTable($tableId);
		if (!in_array($attr[3], $sapiTable['columns'])) {
			throw new WrongParametersException(sprintf("Column '%s' of parameter 'attribute' does not exist in table '%s'", $attr[3], $tableId));
		}

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'name' => $this->params['name'],
				'attribute' => $this->params['attribute'],
				'element' => $this->params['element'],
				'pid' => $this->params['pid'],
				'operator' => $this->params['operator']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], false);

			if (isset($result['job']['result']['uri'])) {
				return $this->createApiResponse(array(
					'uri' => $result['job']['result']['uri']
				));
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'logs' => $result['job']['logs']));
				throw $e;
			}
		}
	}

	/**
	 * Delete user filter
	 *
	 * @Route("/filters")
	 * @Method({"DELETE"})
	 */
	public function deleteFiltersAction()
	{
		$command = 'deleteFilter';
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId', 'uri'));
		$this->checkWriterExistence();

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'uri' => $this->params['uri']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], false);
			if (isset($result['job']['result']['uri'])) {
				return $this->createApiResponse(array(
					'uri' => $result['job']['result']['uri']
				));
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'logs' => $result['job']['logs']));
				throw $e;
			}
		}
	}

	/**
	 * Returns list of filters configured in writer
	 * If 'userEmail' parameter is specified, only returns filters for specified user
	 *
	 * @Route("/filters")
	 * @Method({"GET"})
	 */
	public function getFiltersAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		if (isset($this->params['userEmail'])) {
			if (isset($this->params['pid'])) {
				$filters = $this->getConfiguration()->getFiltersForUser($this->params['userEmail'], $this->params['pid']);
			} else {
				$filters = $this->getConfiguration()->getFiltersForUser($this->params['userEmail']);
			}
		} else {
			$filters = $this->getConfiguration()->getFilters();
		}

		return $this->createApiResponse(array(
			'filters' => $filters
		));
	}



	/**
	 * Assign filter to user
	 *
	 * @Route("/filters-user")
	 * @Route("/filters-users")
	 * @Method({"POST"})
	 */
	public function postFilterUsersAction()
	{
		$command = 'assignFiltersToUser';
		$createdTime = time();

		$this->checkParams(array('writerId', 'filters', 'userEmail', 'pid'));
		$this->checkWriterExistence();

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'filters' => $this->params['filters'],
				'userEmail' => $this->params['userEmail'],
				'pid' => $this->params['pid']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			$result = $this->waitForJob($jobInfo['id'], false);
			if (isset($result['job']['result']['uri'])) {
				return $this->createApiResponse(array(
					'uri' => $result['job']['result']['uri']
				));
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'logs' => $result['job']['logs']));
				throw $e;
			}
		}
	}

	/**
	 * Synchronize filters from writer's configuration to GoodData project
	 *
	 * @Route("/sync-filters")
	 * @Method({"POST"})
	 */
	public function postSyncFiltersAction()
	{
		$command = 'syncFilters';
		$createdTime = time();

		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'pid' => isset($this->params['pid']) ? $this->params['pid'] : null
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			return $this->waitForJob($jobInfo['id']);
		}
	}



	/**
	 * Generates LDM model of writer
	 * @Route("/ldm")
	 * @Method({"GET"})
	 */
	public function getLdmAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		//@TODO return $this->getConfiguration()->getLDM();
	}


	/**
	 * Upload configured date dimension to GoodData
	 *
	 * @Route("/upload-date-dimension")
	 * @Method({"POST"})
	 */
	public function postUploadDateDimensionAction()
	{
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId', 'tableId', 'name'));
		$this->checkWriterExistence();

		$this->getConfiguration()->checkBucketAttributes();
		$dateDimensions = $this->getConfiguration()->getDateDimensions();
		if (!in_array($this->params['name'], array_keys($dateDimensions))) {
			throw new WrongParametersException(sprintf("Date dimension '%s' does not exist in configuration", $this->params['name']));
		}

		$jobInfo = $this->createJob(array(
			'command' => 'UploadDateDimension',
			'createdTime' => date('c', $createdTime),
			'dataset' => $this->params['name'],
			'parameters' => array(
				'name' => $this->params['name'],
				'includeTime' => $dateDimensions[$this->params['name']]['includeTime']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		if (isset($this->params['pid'])) {
			$jobData['parameters']['pid'] = $this->params['pid'];
		}

		$this->enqueue($jobInfo['batchId']);
		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
	}


	/**
	 * Upload dataSet to GoodData
	 *
	 * @Route("/upload-table")
	 * @Method({"POST"})
	 */
	public function postUploadTableAction()
	{
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId', 'tableId'));
		$this->checkWriterExistence();

		$this->getConfiguration()->checkBucketAttributes();

		$batchId = $this->storageApi->generateId();

		$definition = $this->getConfiguration()->getDataSetDefinition($this->params['tableId']);


		// Create date dimensions
		$dateDimensionsToLoad = array();
		$dateDimensions = array();
		if ($definition['columns']) foreach ($definition['columns'] as $column) if ($column['type'] == 'DATE') {
			if (!$dateDimensions) {
				$dateDimensions = $this->getConfiguration()->getDateDimensions();
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
					'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
				);
				if (isset($this->params['pid'])) {
					$jobData['parameters']['pid'] = $this->params['pid'];
				}
				$this->createJob($jobData);
			}
		}

		$tableConfiguration = $this->getConfiguration()->getDataSet($this->params['tableId']);
		$jobData = array(
			'batchId' => $batchId,
			'command' => 'uploadTable',
			'dataset' => !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'],
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'tableId' => $this->params['tableId']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		);
		if (isset($this->params['pid'])) {
			$jobData['parameters']['pid'] = $this->params['pid'];
		}
		if (isset($this->params['incrementalLoad'])) {
			$jobData['parameters']['incrementalLoad'] = $this->params['incrementalLoad'];
		}
		$jobInfo = $this->createJob($jobData);


		$definitionUrl = $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $this->params['tableId']), json_encode($definition));
		$this->sharedConfig->saveJob($jobInfo['id'], array(
			'definition' => $definitionUrl
		));

		$this->enqueue($batchId);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			return $this->waitForJob($jobInfo['id']);
		}
	}

	/**
	 * Upload project to GoodData
	 *
	 * @Route("/upload-project")
	 * @Method({"POST"})
	 */
	public function postUploadProjectAction()
	{
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();
		$this->getConfiguration()->checkBucketAttributes();

		$this->getConfiguration()->getDateDimensions();
		$runId = $this->storageApi->getRunId();
		$batchId = $this->storageApi->generateId();

		$sortedDataSets = $this->getConfiguration()->getSortedDataSets();


		// Create date dimensions
		$dateDimensionsToLoad = array();
		$dateDimensions = array();
		foreach ($sortedDataSets as $dataSet) {
			if ($dataSet['definition']['columns']) foreach ($dataSet['definition']['columns'] as $column) if ($column['type'] == 'DATE') {
				if (!$dateDimensions) {
					$dateDimensions = $this->getConfiguration()->getDateDimensions();
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
						'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
					);
					if (isset($this->params['pid'])) {
						$jobData['parameters']['pid'] = $this->params['pid'];
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
				'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
			);
			if (isset($this->params['pid'])) {
				$jobData['parameters']['pid'] = $this->params['pid'];
			}
			if (isset($this->params['incrementalLoad'])) {
				$jobData['parameters']['incrementalLoad'] = $this->params['incrementalLoad'];
			}
			$jobInfo = $this->createJob($jobData);

			$definitionUrl = $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $dataSet['tableId']), json_encode($dataSet['definition']));
			$this->sharedConfig->saveJob($jobInfo['id'], array(
				'definition' => $definitionUrl
			));
		}

		// Execute reports
		$jobData = array(
			'batchId' => $batchId,
			'runId' => $runId,
			'command' => 'executeReports',
			'createdTime' => date('c', $createdTime),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		);
		$this->createJob($jobData);

		$this->enqueue($batchId);


		if (empty($this->params['wait'])) {
			return $this->getPollResult($batchId, $this->params['writerId'], true);
		} else {
			return $this->waitForBatch($batchId);
		}
	}

	/**
	 * Reset dataSet and remove it from GoodData project
	 *
	 * @Route("/reset-table")
	 * @Method({"POST"})
	 */
	public function postResetTableAction()
	{
		$command = 'resetTable';
		$createdTime = time();

		$this->checkParams(array('writerId', 'tableId'));
		$this->checkWriterExistence();

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'tableId' => $this->params['tableId']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
	}


	/**
	 * Reset GoodData project
	 *
	 * @Route("/reset-project")
	 * @Method({"POST"})
	 */
	public function postResetProjectAction()
	{
		$command = 'resetProject';
		$createdTime = time();

		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'removeClones' => isset($this->params['removeClones'])? (bool)$this->params['removeClones'] : false
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
	}

	/**
	 * Execute Reports in GoodData
	 *
	 * @Route("/execute-reports")
	 * @Method({"POST"})
	 */
	public function postExecuteReportsAction()
	{
		$command = 'executeReports';
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId', 'pid'));
		$this->checkWriterExistence();
		$this->getConfiguration()->checkBucketAttributes();
		$this->getConfiguration()->checkProjectsTable();

		$project = $this->getConfiguration()->getProject($this->params['pid']);
		if (!$project) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $this->params['pid']));
		}

		if (!$project['active']) {
			throw new WrongParametersException(sprintf("Project '%s' is not active", $this->params['pid']));
		}

		$reports = array();
		if (!empty($this->params['reports'])) {
			$reports = (array) $this->params['reports'];

			foreach ($reports AS $reportLink) {
				if (!preg_match('/^\/gdc\/md\/' . $this->params['pid'] . '\//', $reportLink)) {
					throw new WrongParametersException("Parameter 'reports' is not valid; report uri '" .$reportLink . "' does not belong to the project");
				}
			}
		}


		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'pid' => $this->params['pid'],
				'reports' => $reports,
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			return $this->waitForJob($jobInfo['id']);
		}

	}


	/**
	 * Export report data from GoodData
	 *
	 * @Route("/export-report")
	 * @Method({"POST"})
	 */
	public function postExportReportAction()
	{
		$command = 'exportReport';
		$createdTime = time();

		// Init parameters
		$this->checkParams(array('writerId', 'pid', 'report', 'table'));
		$this->checkWriterExistence();

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'pid'       => $this->params['pid'],
				'report'    => $this->params['report'],
				'table'     => $this->params['table']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));

		$this->enqueue($jobInfo['batchId']);

		if (empty($this->params['wait'])) {
			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {
			return $this->waitForJob($jobInfo['id']);
		}
	}



	/**
	 * Get visual model
	 *
	 * @Route("/model")
	 * @Method({"GET"})
	 */
	public function getModelAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		$model = new Graph();
		$dimensionsUrl = sprintf('%s/admin/projects-new/%s/gooddata?config=%s#/date-dimensions',
			$this->container->getParameter('storage_api.url'), $this->getConfiguration()->projectId, $this->getConfiguration()->writerId);
		$tableUrl = sprintf('%s/admin/projects-new/%s/gooddata?config=%s#/table/',
			$this->container->getParameter('storage_api.url'), $this->getConfiguration()->projectId, $this->getConfiguration()->writerId);
		$model->setTableUrl($tableUrl);
		$model->setDimensionsUrl($dimensionsUrl);

		try {
			$result = $model->getGraph($this->getConfiguration());
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
	 *
	 * @Route("/tables")
	 * @Method({"GET"})
	 */
	public function getTablesAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		if (isset($this->params['tableId'])) {
			// Table detail
			return $this->createApiResponse(array(
				'table' => $this->getConfiguration()->getDataSetForApi($this->params['tableId'])
			));
		} elseif (isset($this->params['connection'])) {
			return $this->createApiResponse(array(
				'tables' => $this->getConfiguration()->getDataSetsWithConnectionPoint()
			));
		} else {
			return $this->createApiResponse(array(
				'tables' => $this->getConfiguration()->getDataSets()
			));
		}
	}

	/**
	 * Update tables configuration
	 *
	 * @Route("/tables")
	 * @Method({"POST"})
	 */
	public function postTablesAction()
	{
		$this->checkParams(array('writerId', 'tableId'));
		$this->checkWriterExistence();
		if (!in_array($this->params['tableId'], $this->getConfiguration()->getOutputSapiTables())) {
			throw new WrongParametersException(sprintf("Table '%s' does not exist", $this->params['tableId']));
		}

		$tableId = $this->params['tableId'];
		unset($this->params['tableId']);

		$this->getConfiguration()->updateDataSetsFromSapi();

		if (isset($this->params['column'])) {
			$columnName = trim($this->params['column']);
			unset($this->params['column']);

			// Column detail
			$this->getConfiguration()->updateColumnsDefinition($tableId, $columnName, $this->params);

		} elseif (isset($this->params['columns'])) {
			$this->getConfiguration()->updateColumnsDefinition($tableId, $this->params['columns']);
		} else {
			// Table detail
			$this->getConfiguration()->updateDataSetDefinition($tableId, $this->params);
		}

		return $this->createApiResponse();
	}


	/**
	 * Reset export status of all dataSets and dimensions
	 *
	 * @Route("/reset-export")
	 * @Method({"POST"})
	 */
	public function postResetExportAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		foreach ($this->getConfiguration()->getDataSets() as $dataSet) if (!empty($dataSet['isExported'])) {
			$this->getConfiguration()->updateDataSetDefinition($dataSet['id'], 'isExported', 0);
		}

		return $this->createApiResponse();
	}

	/**
	 * Get all configured date dimensions
	 *
	 * @Route("/date-dimensions")
	 * @Method({"GET"})
	 */
	public function getDateDimensionsAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		return $this->createApiResponse(array(
			'dimensions' => (object) $this->getConfiguration()->getDateDimensions(isset($this->params['usage']))
		));
	}


	/**
	 * Delete configured date dimension
	 *
	 * @Route("/date-dimensions")
	 * @Method({"DELETE"})
	 */
	public function deleteDateDimensionsAction()
	{
		$this->checkParams(array('writerId', 'name'));
		$this->checkWriterExistence();

		$dimensions = $this->getConfiguration()->getDateDimensions();
		if (isset($dimensions[$this->params['name']])) {
			$this->getConfiguration()->deleteDateDimension($this->params['name']);
			return $this->createApiResponse();
		} else {
			throw new WrongParametersException(sprintf("Dimension '%s' does not exist", $this->params['name']));
		}
	}

	/**
	 * Update configured date dimension or create new
	 *
	 * @Route("/date-dimensions")
	 * @Method({"POST"})
	 */
	public function postDateDimensionsAction()
	{
		$this->checkParams(array('writerId', 'name'));
		$this->checkWriterExistence();

		$this->params['name'] = trim($this->params['name']);

		$dimensions = $this->getConfiguration()->getDateDimensions();
		if (!isset($dimensions[$this->params['name']])) {
			$this->getConfiguration()->saveDateDimension($this->params['name'], !empty($this->params['includeTime']));
		}

		return $this->createApiResponse();
	}



	/***********************
	 * @section Jobs
	 */

	/**
	 * Get Jobs
	 * Allow filtering by days, command and tableId
	 *
	 * @Route("/jobs")
	 * @Method({"GET"})
	 */
	public function getJobsAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		if (empty($this->params['jobId'])) {
			$days = isset($this->params['days']) ? $this->params['days'] : 7;
			$tableId = empty($this->params['tableId']) ? null : $this->params['tableId'];
			$command = empty($this->params['command']) ? null : $this->params['command'];
			$tokenId = empty($this->params['tokenId']) ? null : $this->params['tokenId'];
			$status = empty($this->params['status']) ? null : $this->params['status'];
			$jobs = $this->getSharedConfig()->fetchJobs($this->getConfiguration()->projectId, $this->params['writerId'], $days, $tableId);

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

			return $this->createApiResponse(array(
				'jobs' => $result
			));
		} else {
			if (is_array($this->params['jobId'])) {
				throw new WrongParametersException("Parameter 'jobId' has to be a number");
			}
			$job = $this->getSharedConfig()->fetchJob($this->params['jobId'], $this->getConfiguration()->writerId, $this->getConfiguration()->projectId);
			if (!$job) {
				throw new WrongParametersException(sprintf("Job '%d' not found", $this->params['jobId']));
			}

			$job = $this->getSharedConfig()->jobToApiResponse($job, $this->getS3Client());
			return $this->createApiResponse(array(
				'job' => $job
			));
		}
	}

	/**
	 * Get Batch
	 *
	 * @Route("/batch")
	 * @Method({"GET"})
	 */
	public function getBatchAction()
	{
		$this->checkParams(array('writerId', 'batchId'));
		$this->checkWriterExistence();

		return $this->createApiResponse(array(
			'batch' => $this->getSharedConfig()->batchToApiResponse($this->params['batchId'], $this->getS3Client())
		));
	}

	/**
	 * Cancel waiting jobs
	 *
	 * @Route("/cancel-jobs")
	 * @Method({"POST"})
	 */
	public function postCancelJobsAction()
	{
		$this->checkParams(array('writerId'));

		$this->getSharedConfig()->cancelJobs($this->getConfiguration()->projectId, $this->getConfiguration()->writerId);
		return $this->createApiResponse();
	}

	/**
	 * Migrate old configuration
	 * @TODO REMOVE SOON
	 *
	 * @Route("/migrate-configuration")
	 * @Method({"POST"})
	 */
	public function postMigrateConfigurationAction()
	{
		$this->checkParams(array('writerId'));
		$this->getConfiguration()->migrateConfiguration();
	}




	/***************************************************************************
	 * *************************************************************************
	 * @section Helpers
	 */

	private function createApiResponse($response = array(), $statusCode = 200)
	{
		$event = $this->stopWatch->stop('request');
		$responseBody = array(
			'status'    => 'ok',
			'duration'  => $event->getDuration()
		);

		if (null != $response) {
			$responseBody = array_merge($response, $responseBody);
		}

		return $this->createJsonResponse($responseBody, $statusCode);
	}

	private function getPollResult($id, $writerId, $isBatch = false)
	{
		/** @var \Symfony\Component\Routing\RequestContext $context */
		$context = $this->container->get('router')->getContext();

		return $this->createApiResponse(array(
			($isBatch? 'batch' : 'job') => (int)$id,
			'url' => sprintf('https://%s%s/gooddata-writer/%s?writerId=%s&%s=%s', $context->getHost(), $context->getBaseUrl(),
				$isBatch? 'batch' : 'jobs', $writerId, $isBatch? 'batchId' : 'jobId', $id)
		), 202);
	}

	private function getConfiguration()
	{
		if (!isset($this->params['writerId'])) {
			throw new WrongParametersException('Parameter \'writerId\' is missing');
		}
		if (!$this->configuration) {
			$this->configuration = new Configuration($this->storageApi, $this->params['writerId'], $this->appConfiguration->scriptsPath);
		}
		return $this->configuration;
	}

	private function getS3Client()
	{
		if (!$this->s3Client) {
			if (!$this->appConfiguration) {
				$this->appConfiguration = $this->container->get('gooddata_writer.app_configuration');
			}
			$this->s3Client = new S3Client(
				$this->appConfiguration,
				$this->getConfiguration()->projectId . '.' . $this->getConfiguration()->writerId
			);
		}
		return $this->s3Client;
	}

	private function getWriterQueue()
	{
		if (!$this->queue) {
			if (!$this->appConfiguration) {
				$this->appConfiguration = $this->container->get('gooddata_writer.app_configuration');
			}
			$this->queue = $this->container->get('gooddata_writer.jobs_queue');
		}
		return $this->queue;
	}

	private function getSharedConfig()
	{
		if (!$this->sharedConfig) {
			if (!$this->appConfiguration) {
				$this->appConfiguration = $this->container->get('gooddata_writer.app_configuration');
			}
			$this->sharedConfig = $this->container->get('gooddata_writer.shared_config');
		}
		return $this->sharedConfig;
	}

	private function checkParams($required)
	{
		foreach($required as $k) {
			if (empty($this->params[$k])) {
				throw new WrongParametersException("Parameter '" . $k . "' is missing");
			}
		}
	}

	private function checkWriterExistence()
	{
		if (!$this->getConfiguration()->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $this->params['writerId']));
		}
	}

	private function createJob($params)
	{
		$jobId = $this->storageApi->generateId();
		if (!isset($params['batchId'])) {
			$params['batchId'] = $jobId;
		}

		$params['queueId'] = sprintf('%s.%s.%s', $this->getConfiguration()->projectId, $this->getConfiguration()->writerId,
			isset($params['queue']) ? $params['queue'] : SharedConfig::PRIMARY_QUEUE);
		unset($params['queue']);

		$jobInfo = array(
			'id' => $jobId,
			'runId' => $this->storageApi->getRunId(),
			'projectId' => $this->getConfiguration()->projectId,
			'writerId' => $this->getConfiguration()->writerId,
			'token' => $this->storageApi->token,
			'tokenId' => $this->getConfiguration()->tokenInfo['id'],
			'tokenDesc' => $this->getConfiguration()->tokenInfo['description'],
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
			'projectIdWriterId' => sprintf('%s.%s', $this->getConfiguration()->projectId, $this->getConfiguration()->writerId)
		);
		$jobInfo = array_merge($jobInfo, $params);

		$this->getSharedConfig()->saveJob($jobId, $jobInfo);

		$this->container->get('logger')->log(Logger::INFO, 'Job created ' . $jobId, array(
			'writerId' => $this->getConfiguration()->writerId,
			'runId' => $jobInfo['runId'],
			'command' => $jobInfo['command'],
			'params' => $this->params
		));

		return $jobInfo;
	}

	/**
	 * @param $batchId
	 */
	protected function enqueue($batchId)
	{
		$this->getWriterQueue()->enqueue(array(
			'projectId' => $this->getConfiguration()->projectId,
			'writerId' => $this->getConfiguration()->writerId,
			'batchId' => $batchId
		));
	}


	protected function waitForJob($jobId, $returnResponse = true)
	{
		$jobFinished = false;
		$i = 1;
		do {
			$job = $this->getSharedConfig()->fetchJob($jobId, $this->getConfiguration()->writerId, $this->getConfiguration()->projectId);
			if (!$job) {
				throw new WrongParametersException(sprintf("Job '%d' not found", $this->params['jobId']));
			}
			$jobInfo = $this->getSharedConfig()->jobToApiResponse($job, $this->getS3Client());
			if (isset($jobInfo['status']) && !in_array($jobInfo['status'], array('waiting', 'processing'))) {
				$jobFinished = true;
			}
			if (!$jobFinished) sleep($i * 10);
			$i++;
		} while(!$jobFinished);

		if ($returnResponse) {
			if ($jobInfo['status'] == 'success') {
				return $this->createApiResponse($jobInfo);
			} else {
				$e = new JobProcessException('Job processing failed');
				$e->setData(array('result' => $jobInfo['result'], 'logs' => $jobInfo['logs']));
				throw $e;
			}
		} else {
			return $jobInfo;
		}
	}

	protected function waitForBatch($batchId)
	{
		$jobsFinished = false;
		$i = 1;
		do {
			$jobsInfo = $this->getSharedConfig()->batchToApiResponse($batchId, $this->getS3Client());
			if (isset($jobsInfo['status']) && !in_array($jobsInfo['status'], array('waiting', 'processing'))) {
				$jobsFinished = true;
			}
			if (!$jobsFinished) sleep($i * 10);
			$i++;
		} while(!$jobsFinished);

		if ($jobsInfo['status'] == 'success') {
			return $this->createApiResponse($jobsInfo);
		} else {
			$e = new JobProcessException('Batch processing failed');
			$e->setData(array('result' => $jobsInfo['result'], 'logs' => $jobsInfo['logs']));
			throw $e;
		}
	}

} 