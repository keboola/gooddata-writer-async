<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-03-14
 */

namespace Keboola\GoodDataWriter\Controller;


use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Writer\SharedConfigException;
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
	const STOPWATCH_NAME_REQUEST = 'requestTimer';

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

	/**
	 * @var \Symfony\Component\Translation\Translator
	 */
	private $translator;
	/**
	 * @var EventLogger
	 */
	private $eventLogger;

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

		$this->translator = $this->container->get('translator');
		$this->appConfiguration = $this->container->get('gooddata_writer.app_configuration');

		if (!defined('JSON_PRETTY_PRINT')) {
			// fallback for PHP <= 5.3
			define('JSON_PRETTY_PRINT', 0);
		}
		set_time_limit(3 * 60 * 60);

		// Get params
		$request = $this->getRequest();
		$this->method = $request->getMethod();
		$this->params = in_array($this->method, array('POST', 'PUT'))? $this->getPostJson($request) : $request->query->all();
		array_walk_recursive($this->params, function(&$param) {
			$param = trim($param);
		});

		if (isset($this->params['queue']) && !in_array($this->params['queue'], array(SharedConfig::PRIMARY_QUEUE, SharedConfig::SECONDARY_QUEUE))) {
			throw new WrongParametersException($this->translator->trans('parameters.queue %1', array('%1' => SharedConfig::PRIMARY_QUEUE . ', ' . SharedConfig::SECONDARY_QUEUE)));
		}

		$this->eventLogger = new EventLogger($this->appConfiguration, $this->storageApi);

		$this->stopWatch = new Stopwatch();
		$this->stopWatch->start(self::STOPWATCH_NAME_REQUEST);
	}

	public function __destruct()
	{
		$params = array();
		foreach ($this->params as $k => $p) {
			$params[$k] = ($k == 'password')? '***' : $p;
		}
		/** @var \Symfony\Bundle\FrameworkBundle\Routing\Router $router */
		$router = $this->get('router');
		if ($this->eventLogger) {
			$this->eventLogger->log(
				isset($this->params['writerId']) ? $this->params['writerId'] : null,
				$this->storageApi->getRunId(),
				'Called API ' . $router->getContext()->getMethod() . ' ' . $router->getContext()->getPathInfo(),
				null,
				$params
			);
		}
	}



	/**
	 * Optimize SLI Hash
	 *
	 * @TODO support for writer clones
	 * @Route("/optimize-sli-hash")
	 * @Method({"POST"})
	 */
	public function postOptimizeSliHashAction()
	{
		$command = 'optimizeSliHash';
		$createdTime = time();
		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(),
			'queue' => SharedConfig::SERVICE_QUEUE
		));
		$this->enqueue($jobInfo['batchId']);

		$this->getConfiguration()->updateWriter('maintenance', 1);
		$this->getSharedConfig()->setWriterStatus($this->getConfiguration()->projectId, $this->params['writerId'], SharedConfig::WRITER_STATUS_MAINTENANCE);

		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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

		$tokenInfo = $this->storageApi->getLogData();
		$projectId = $tokenInfo['owner']['id'];

		$this->checkParams(array('writerId'));
		if (!preg_match('/^[a-zA-Z0-9_]+$/', $this->params['writerId'])) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.format'));
		}
		if (strlen($this->params['writerId'] > 50)) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.length'));
		}
		if ($this->getSharedConfig()->writerExists($projectId, $this->params['writerId'])) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.exists'));
		}


		$batchId = $this->storageApi->generateId();
		$jobData = array(
			'batchId' => $batchId,
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array()
		);

		if (!empty($this->params['username']) || !empty($this->params['password']) || !empty($this->params['pid'])) {
			if (empty($this->params['username'])) {
				throw new WrongParametersException($this->translator->trans('parameters.username_missing'));
			}
			if (empty($this->params['password'])) {
				throw new WrongParametersException($this->translator->trans('parameters.password_missing'));
			}
			if (empty($this->params['pid'])) {
				throw new WrongParametersException($this->translator->trans('parameters.pid_missing'));
			}

			$jobData['parameters']['pid'] = $this->params['pid'];
			$jobData['parameters']['username'] = $this->params['username'];
			$jobData['parameters']['password'] = $this->params['password'];

			/** @var RestApi $restApi */
			$restApi = $this->container->get('gooddata_writer.rest_api');
			try {
				$restApi->login($this->params['username'], $this->params['password']);
			} catch (\Exception $e) {
				throw new WrongParametersException($this->translator->trans('parameters.gd.credentials'));
			}
			if (!$restApi->hasAccessToProject($this->params['pid'])) {
				throw new WrongParametersException($this->translator->trans('parameters.gd.project_inaccessible'));
			}
			if (!in_array('admin', $restApi->getUserRolesInProject($this->params['username'], $this->params['pid']))) {
				throw new WrongParametersException($this->translator->trans('parameters.gd.user_not_admin'));
			}
		} else {
			$jobData['parameters']['accessToken'] = !empty($this->params['accessToken'])? $this->params['accessToken'] : $this->appConfiguration->gd_accessToken;
			$jobData['parameters']['projectName'] = sprintf($this->appConfiguration->gd_projectNameTemplate, $tokenInfo['owner']['name'], $this->params['writerId']);
		}

		$this->getConfiguration()->createWriter($this->params['writerId']);

		$jobInfo = $this->createJob($jobData);

		if(empty($this->params['users'])) {
			$this->enqueue($batchId);

			return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
		} else {

			$users = is_array($this->params['users'])? $this->params['users'] : explode(',', $this->params['users']);
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
	 * Update writer attributes
	 *
	 * @Route("/writers/{writerId}")
	 * @Method({"POST"})
	 */
	public function updateWriterAction($writerId)
	{
		$this->params['writerId'] = $writerId;

		$this->checkWriterExistence();

		// Update writer configuration
		foreach ($this->params as $key => $value) if ($key != 'writerId') {
			if (is_array($value)) $value = json_encode($value);
			$this->getConfiguration()->updateWriter($key, $value);
		}
		return $this->createApiResponse();
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
		$this->getConfiguration()->updateWriter('delete', '1');

		$this->getSharedConfig()->cancelJobs($this->getConfiguration()->projectId, $this->getConfiguration()->writerId);

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(),
			'queue' => SharedConfig::SERVICE_QUEUE
		));
		$this->enqueue($jobInfo['batchId']);


		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
	}

	/**
	 * Detail writer or list all writers
	 *
	 * @Route("/writers")
	 * @Route("/writers/{writerId}")
	 * @Method({"GET"})
	 */
	public function getWritersAction($writerId=null)
	{
		if ($writerId) $this->params['writerId'] = $writerId;

		if (isset($this->params['writerId'])) {
			$this->checkWriterExistence();

			$configuration = $this->getConfiguration()->formatWriterAttributes($this->getConfiguration()->bucketId, $this->getConfiguration()->bucketAttributes());
			/*try {
				$sharedConfiguration = $this->getSharedConfig()->getWriter($this->getConfiguration()->projectId, $this->params['writerId']);
			} catch (SharedConfigException $e) {
				//@TODO temporary fix
				$this->getSharedConfig()->createWriter($this->getConfiguration()->projectId, $this->params['writerId']);
				$sharedConfiguration = array('status' => SharedConfig::WRITER_STATUS_READY, 'createdTime' => '');
			}*/$sharedConfiguration = array();
			return $this->createApiResponse(array(
				'writer' => array_merge($configuration, $sharedConfiguration)
			));
		} else {
			$configuration = new Configuration($this->storageApi, $this->getSharedConfig());
			$tokenInfo = $this->storageApi->getLogData();
			$result = array();
			foreach ($configuration->getWriters() as $writer) {
				/*try {
					$sharedConfiguration = $this->getSharedConfig()->getWriter($tokenInfo['owner']['id'], $writer['id']);
				} catch (SharedConfigException $e) {
					//@TODO temporary fix
					$this->getSharedConfig()->createWriter($tokenInfo['owner']['id'], $writer['id']);
					$sharedConfiguration = array('status' => SharedConfig::WRITER_STATUS_READY, 'createdTime' => '');
				}*/$sharedConfiguration = array();
				$result[] = array_merge($writer, $sharedConfiguration);
			}

			return $this->createApiResponse(array(
				'writers' => $result
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


		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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
			throw new WrongParametersException($this->translator->trans('parameters.role %1', array('%1' => implode(', ', $allowedRoles))));
		}
		if (!$this->getConfiguration()->getProject($this->params['pid'])) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
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


		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}
		if (!$this->getConfiguration()->isProjectUser($this->params['email'], $this->params['pid'])) {
			throw new WrongParametersException($this->translator->trans('parameters.email_not_configured'));
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

		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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
			throw new WrongParametersException($this->translator->trans('parameters.password_length'));
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
				'ssoProvider' => empty($this->params['ssoProvider'])? null : $this->params['ssoProvider']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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
		$this->checkParams(array('writerId', 'email', 'pid'));
		$this->checkWriterExistence();

		if (!$this->getSharedConfig()->projectBelongsToWriter($this->getConfiguration()->projectId, $this->getConfiguration()->writerId, $this->params['pid'])) {
			throw new WrongParametersException($this->translator->trans('parameters.sso_wrong_pid'));
		}

		if (!empty($this->params['createUser']) && $this->params['createUser'] == 1 && !$this->getConfiguration()->getUser($this->params['email'])) {
			$result = $this->postUsersAction();
			$jsonResult = json_decode($result->getContent(), true);
			$jobFinished = false;
			$i = 1;
			do {
				$job = $this->getSharedConfig()->fetchJob($jsonResult['job'], $this->getConfiguration()->writerId, $this->getConfiguration()->projectId);
				if (!$job) {
					throw new WrongParametersException(sprintf("Job '%d' not found", $this->params['jobId']));
				}
				$jobInfo = $this->getSharedConfig()->jobToApiResponse($job, $this->getS3Client());
				if (isset($jobInfo['status']) && SharedConfig::isJobFinished($jobInfo['status'])) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep($i * 5);
				$i++;
			} while(!$jobFinished);

			if ($jobInfo['status'] == SharedConfig::JOB_STATUS_SUCCESS) {
				if (!empty($jobInfo['result']['alreadyExists'])) {
					throw new JobProcessException($this->translator->trans('result.cancelled'));
				}
				// Do nothing
			} elseif ($jobInfo['status'] == SharedConfig::JOB_STATUS_CANCELLED) {
				throw new JobProcessException($this->translator->trans('result.cancelled'));
			} else {
				$e = new JobProcessException(!empty($jobInfo['result']['error'])? $jobInfo['result']['error'] : $this->translator->trans('result.unknown'));
				$e->setData(array('result' => $jobInfo['result'], 'logs' => $jobInfo['logs']));
				throw $e;
			}


			$result = $this->postProjectUsersAction();
			$jsonResult = json_decode($result->getContent(), true);
			$jobFinished = false;
			$i = 1;
			do {
				$job = $this->getSharedConfig()->fetchJob($jsonResult['job'], $this->getConfiguration()->writerId, $this->getConfiguration()->projectId);
				if (!$job) {
					throw new WrongParametersException(sprintf("Job '%d' not found", $this->params['jobId']));
				}
				$jobInfo = $this->getSharedConfig()->jobToApiResponse($job, $this->getS3Client());
				if (isset($jobInfo['status']) && SharedConfig::isJobFinished($jobInfo['status'])) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep($i * 5);
				$i++;
			} while(!$jobFinished);

			if ($jobInfo['status'] == SharedConfig::JOB_STATUS_SUCCESS) {
				// Do nothing
			} elseif ($jobInfo['status'] == SharedConfig::JOB_STATUS_CANCELLED) {
				throw new JobProcessException($this->translator->trans('result.cancelled'));
			} else {
				$e = new JobProcessException(!empty($jobInfo['result']['error'])? $jobInfo['result']['error'] : $this->translator->trans('result.unknown'));
				$e->setData(array('result' => $jobInfo['result'], 'logs' => $jobInfo['logs']));
				throw $e;
			}
		}

		if (!$this->getSharedConfig()->userBelongsToWriter($this->getConfiguration()->projectId, $this->getConfiguration()->writerId, $this->params['email'])) {
			throw new WrongParametersException($this->translator->trans('parameters.sso_wrong_email'));
		}

		$targetUrl = isset($this->params['targetUrl'])? $this->params['targetUrl'] : '/#s=/gdc/projects/' . $this->params['pid'];
		$validity = (isset($this->params['validity']))? $this->params['validity'] : 86400;

		$domainUser = $this->getSharedConfig()->getDomainUser($this->appConfiguration->gd_domain);
		$sso = new SSO($domainUser->username, $this->appConfiguration);
		$ssoLink = $sso->url($targetUrl, $this->params['email'], $validity);

		if (null == $ssoLink) {
			$e = new SyrupComponentException(500, $this->translator->trans('error.sso_unknown'));
			$e->setData(array('params' => $this->params));
			throw $e;
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

		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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
			throw new WrongParametersException($this->translator->trans('parameters.query'));
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
		//@TODO backwards compatibility, REMOVE SOON
		if (isset($this->params['element'])) {
			$this->params['value'] = $this->params['element'];
			unset($this->params['element']);
		}
		$this->checkParams(array('writerId', 'name', 'attribute', 'value', 'pid'));
		$this->checkWriterExistence();
		if (!isset($this->params['operator'])) {
			$this->params['operator'] = '=';
		}

		if ($this->getConfiguration()->getFilter($this->params['name'])) {
			throw new WrongParametersException($this->translator->trans('parameters.filter.already_exists'));
		}

		$attr = explode('.', $this->params['attribute']);
		if (count($attr) != 4) {
			throw new WrongParametersException($this->translator->trans('parameters.attribute.format'));
		}
		$tableId = sprintf('%s.%s.%s', $attr[0], $attr[1], $attr[2]);
		$sapiTable = $this->getConfiguration()->getSapiTable($tableId);
		if (!in_array($attr[3], $sapiTable['columns'])) {
			throw new WrongParametersException($this->translator->trans('parameters.attribute.not_found'));
		}

		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'name' => $this->params['name'],
				'attribute' => $this->params['attribute'],
				'value' => $this->params['value'],
				'pid' => $this->params['pid'],
				'operator' => $this->params['operator']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));
		$this->enqueue($jobInfo['batchId']);


		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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

		$this->checkWriterExistence();

		if (isset($this->params['name'])) {
			if (!$this->getConfiguration()->getFilter($this->params['name'])) {
				throw new WrongParametersException($this->translator->trans('parameters.filters.not_exist %1', array('%1' => $this->params['name'])));
			}
			$this->checkParams(array('writerId'));
		} else {
			//@TODO backwards compatibility, REMOVE SOON
			$this->checkParams(array('writerId', 'uri'));
			if (!$this->getConfiguration()->checkFilterUri($this->params['uri'])) {
				throw new WrongParametersException($this->translator->trans('parameters.filters.not_exist %1', array('%1' => $this->params['uri'])));
			}
		}

		$jobData = array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		);
		if (isset($this->params['name'])) {
			$jobData['parameters']['name'] = $this->params['name'];
		} else {
			$jobData['parameters']['uri'] = $this->params['uri'];
		}
		$jobInfo = $this->createJob($jobData);
		$this->enqueue($jobInfo['batchId']);


		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
	}

	/**
	 * Returns list of filters configured in writer
	 * Can be filtered by email or pid
	 *
	 * @Route("/filters")
	 * @Method({"GET"})
	 */
	public function getFiltersAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		if (isset($this->params['userEmail'])) {
			$this->params['email'] = $this->params['userEmail'];
			unset($this->params['userEmail']);
		}

		if (isset($this->params['email'])) {
			$filters = $this->getConfiguration()->getFiltersForUser($this->params['email']);
		} elseif (isset($this->params['pid'])) {
			$filters = $this->getConfiguration()->getFiltersForProject($this->params['pid']);
		} else {
			$filters = $this->getConfiguration()->getFilters();
		}

		return $this->createApiResponse(array(
			'filters' => $filters
		));
	}

	/**
	 * Returns list of filters in projects
	 * Can be filtered by pid
	 *
	 * @Route("/filters-projects")
	 * @Method({"GET"})
	 */
	public function getFiltersProjectsAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		if (isset($this->params['pid'])) {
			$filters = $this->getConfiguration()->getFiltersProjectsByPid($this->params['pid']);
		} elseif (isset($this->params['filter'])) {
			$filters = $this->getConfiguration()->getFiltersProjectsByFilter($this->params['filter']);
		} else {
			$filters = $this->getConfiguration()->getFiltersProjects();
		}

		return $this->createApiResponse(array(
			'filters' => $filters
		));
	}


	/**
	 * Returns list of filters for users
	 * Can be filtered by email
	 *
	 * @Route("/filters-users")
	 * @Method({"GET"})
	 */
	public function getFiltersUsersAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		if (isset($this->params['email'])) {
			$filters = $this->getConfiguration()->getFiltersUsersByEmail($this->params['email']);
		} elseif (isset($this->params['filter'])) {
			$filters = $this->getConfiguration()->getFiltersUsersByFilter($this->params['filter']);
		} else {
			$filters = $this->getConfiguration()->getFiltersUsers();
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

		//@TODO backwards compatibility, REMOVE SOON
		if (isset($this->params['userEmail'])) {
			$this->params['email'] = $this->params['userEmail'];
			unset($this->params['userEmail']);
		}
		////

		$this->checkParams(array('writerId', 'email'));
		if (!isset($this->params['filters'])) {
			throw new WrongParametersException($this->translator->trans('parameters.filters.required'));
		}
		$configuredFilters = array();
		foreach ($this->getConfiguration()->getFilters() as $f) {
			$configuredFilters[] = $f['name'];
		}
		foreach ($this->params['filters'] as $f) {
			if (!in_array($f, $configuredFilters)) {
				throw new WrongParametersException($this->translator->trans('parameters.filters.not_exist %1', array('%1' => $f)));
			}
		}
		$this->checkWriterExistence();

		$batchId = $this->storageApi->generateId();
		$this->createJob(array(
			'batchId' => $batchId,
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'filters' => $this->params['filters'],
				'email' => $this->params['email']
			),
			'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
		));

		$this->enqueue($batchId);
		return $this->getPollResult($batchId, $this->params['writerId'], true);
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

		$batchId = $this->storageApi->generateId();
		$projects = empty($this->params['pid'])? $this->getProjectsToUse() : array($this->params['pid']);
		foreach ($projects as $pid) {
			$this->createJob(array(
				'batchId' => $batchId,
				'command' => $command,
				'createdTime' => date('c', $createdTime),
				'parameters' => array(
					'pid' => $pid
				),
				'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
			));
		}

		$this->enqueue($batchId);
		return $this->getPollResult($batchId, $this->params['writerId'], true);
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
			throw new WrongParametersException($this->translator->trans('parameters.dimension_name'));
		}

		$batchId = $this->storageApi->generateId();
		foreach ($this->getProjectsToUse() as $pid) {
			$this->createJob(array(
				'batchId' => $batchId,
				'command' => 'UploadDateDimension',
				'createdTime' => date('c', $createdTime),
				'dataset' => $this->params['name'],
				'parameters' => array(
					'pid' => $pid,
					'name' => $this->params['name'],
					'includeTime' => $dateDimensions[$this->params['name']]['includeTime']
				),
				'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
			));
		}

		$this->enqueue($batchId);
		return $this->getPollResult($batchId, $this->params['writerId'], true);
	}

	/**
	 * @Route("/update-model")
	 * @Method({"POST"})
	 * params: pid, queue
	 */
	public function postUpdateModel()
	{
		$createdTime = time();

		$this->checkParams(array('writerId', 'tableId'));
		$this->checkWriterExistence();
		$this->getConfiguration()->checkBucketAttributes();

		$definition = $this->getConfiguration()->getDataSetDefinition($this->params['tableId']);
		$tableConfiguration = $this->getConfiguration()->getDataSet($this->params['tableId']);

		$batchId = $this->storageApi->generateId();
		foreach ($this->getProjectsToUse() as $pid) {
			$jobInfo = $this->createJob(array(
				'batchId' => $batchId,
				'command' => 'updateModel',
				'dataset' => !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'],
				'createdTime' => date('c', $createdTime),
				'parameters' => array(
					'pid' => $pid,
					'tableId' => $this->params['tableId']
				),
				'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
			));
			$definitionUrl = $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $this->params['tableId']), json_encode($definition));
			$this->sharedConfig->saveJob($jobInfo['id'], array(
				'definition' => $definitionUrl
			));
		}

		$this->enqueue($batchId);
		return $this->getPollResult($batchId, $this->params['writerId'], true);
	}

	/**
	 * @Route("/load-data")
	 * @Method({"POST"})
	 * params: pid, queue
	 */
	public function postLoadData()
	{
		$createdTime = time();

		$this->checkParams(array('writerId', 'tables'));
		$this->checkWriterExistence();
		$this->getConfiguration()->checkBucketAttributes();

		$batchId = $this->storageApi->generateId();
		foreach ($this->params['tables'] as $tableId) {
			$definition = $this->getConfiguration()->getDataSetDefinition($tableId);
			$tableConfiguration = $this->getConfiguration()->getDataSet($tableId);
			foreach ($this->getProjectsToUse() as $pid) {
				$jobData = array(
					'batchId' => $batchId,
					'command' => 'loadData',
					'dataset' => !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'],
					'createdTime' => date('c', $createdTime),
					'parameters' => array(
						'pid' => $pid,
						'tableId' => $tableId
					),
					'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
				);
				if (isset($this->params['incrementalLoad'])) {
					$jobData['parameters']['incrementalLoad'] = $this->params['incrementalLoad'];
				}
				$jobInfo = $this->createJob($jobData);
				$definitionUrl = $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $tableId), json_encode($definition));
				$this->sharedConfig->saveJob($jobInfo['id'], array(
					'definition' => $definitionUrl
				));
			}
		}

		$this->enqueue($batchId);
		return $this->getPollResult($batchId, $this->params['writerId'], true);
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
		$projectsToUse = $this->getProjectsToUse();


		// Create date dimensions
		$dateDimensionsToLoad = array();
		$dateDimensions = array();
		if ($definition['columns']) foreach ($definition['columns'] as $column) if ($column['type'] == 'DATE') {
			if (!$dateDimensions) {
				$dateDimensions = $this->getConfiguration()->getDateDimensions();
			}

			$dimension = $column['schemaReference'];
			if (!isset($dateDimensions[$dimension])) {
				throw new WrongParametersException($this->translator->trans('configuration.dimension_not_found %d %c', array('%d' => $dimension, '%c' => $column['name'])));
			}

			if (!$dateDimensions[$dimension]['isExported'] && !in_array($dimension, $dateDimensionsToLoad)) {
				$dateDimensionsToLoad[] = $dimension;

				foreach ($projectsToUse as $pid) {
					$jobData = array(
						'batchId' => $batchId,
						'command' => 'uploadDateDimension',
						'dataset' => $dimension,
						'createdTime' => date('c', $createdTime),
						'parameters' => array(
							'pid' => $pid,
							'name' => $dimension,
							'includeTime' => $dateDimensions[$dimension]['includeTime']
						),
						'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
					);
					$this->createJob($jobData);
				}
			}
		}

		/** @var RestApi $restApi */
		$restApi = $this->container->get('gooddata_writer.rest_api');
		$bucketAttributes = $this->getConfiguration()->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$tableConfiguration = $this->getConfiguration()->getDataSet($this->params['tableId']);
		foreach ($projectsToUse as $pid) {

			$dataSetName = !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'];
			$dataSetId = Model::getDatasetId($dataSetName);

			// Make decision about creating / updating of data set
			$existingDataSets = $restApi->getDataSets($pid);
			$dataSetExists = in_array($dataSetId, array_keys($existingDataSets));
			$lastGoodDataUpdate = empty($existingDataSets[$dataSetId]['lastChangeDate'])? null : Model::getTimestampFromApiDate($existingDataSets[$dataSetId]['lastChangeDate']);
			$lastConfigurationUpdate = empty($tableConfiguration['lastChangeDate'])? null : strtotime($tableConfiguration['lastChangeDate']);
			$doUpdate = $dataSetExists && $lastConfigurationUpdate && (!$lastGoodDataUpdate || $lastGoodDataUpdate < $lastConfigurationUpdate);

			if (!$dataSetExists || $doUpdate) {
				$jobData = array(
					'batchId' => $batchId,
					'command' => 'updateModel',
					'dataset' => !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'],
					'createdTime' => date('c', $createdTime),
					'parameters' => array(
						'pid' => $pid,
						'tableId' => $this->params['tableId']
					),
					'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
				);
				$jobInfo = $this->createJob($jobData);

				$definitionUrl = $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $this->params['tableId']), json_encode($definition));
				$this->sharedConfig->saveJob($jobInfo['id'], array(
					'definition' => $definitionUrl
				));
			}

			$jobData = array(
				'batchId' => $batchId,
				'command' => 'loadData',
				'dataset' => $dataSetName,
				'createdTime' => date('c', $createdTime),
				'parameters' => array(
					'pid' => $pid,
					'tableId' => $this->params['tableId']
				),
				'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
			);
			if (isset($this->params['incrementalLoad'])) {
				$jobData['parameters']['incrementalLoad'] = $this->params['incrementalLoad'];
			}
			$jobInfo = $this->createJob($jobData);

			$definitionUrl = $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $this->params['tableId']), json_encode($definition));
			$this->sharedConfig->saveJob($jobInfo['id'], array(
				'definition' => $definitionUrl
			));
		}

		$this->enqueue($batchId);


		return $this->getPollResult($batchId, $this->params['writerId'], true, !empty($jobInfo)? $jobInfo['id'] : null);
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
		$projectsToUse = $this->getProjectsToUse();

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
					throw new WrongParametersException($this->translator->trans('configuration.dimension_not_found %d %c %t', array('%d' => $dimension, '%c' => $column['name'], '%t' => $dataSet['tableId'])));
				}

				if (!$dateDimensions[$dimension]['isExported'] && !in_array($dimension, $dateDimensionsToLoad)) {
					$dateDimensionsToLoad[] = $dimension;

					foreach ($projectsToUse as $pid) {
						$jobData = array(
							'batchId' => $batchId,
							'command' => 'uploadDateDimension',
							'dataset' => $dimension,
							'createdTime' => date('c', $createdTime),
							'parameters' => array(
								'pid' => $pid,
								'name' => $dimension,
								'includeTime' => $dateDimensions[$dimension]['includeTime']
							),
							'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
						);
						$this->createJob($jobData);
					}
				}
			}
		}


		/** @var RestApi $restApi */
		$restApi = $this->container->get('gooddata_writer.rest_api');
		$bucketAttributes = $this->getConfiguration()->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$existingDataSets = array();

		foreach ($sortedDataSets as $dataSet) {
			foreach ($projectsToUse as $pid) {
				$dataSetId = Model::getDatasetId($dataSet['title']);

				// Make decision about creating / updating of data set
				if (!isset($existingDataSets[$pid])) {
					$existingDataSets[$pid] = $restApi->getDataSets($pid);
				}
				$dataSetExists = in_array($dataSetId, array_keys($existingDataSets[$pid]));
				$lastGoodDataUpdate = empty($existingDataSets[$pid][$dataSetId]['lastChangeDate'])? null : Model::getTimestampFromApiDate($existingDataSets[$pid][$dataSetId]['lastChangeDate']);
				$lastConfigurationUpdate = empty($dataSet['lastChangeDate'])? null : strtotime($dataSet['lastChangeDate']);
				$doUpdate = $dataSetExists && $lastConfigurationUpdate && (!$lastGoodDataUpdate || $lastGoodDataUpdate < $lastConfigurationUpdate);

				if (!$dataSetExists || $doUpdate) {
					$jobData = array(
						'batchId' => $batchId,
						'runId' => $runId,
						'command' => 'updateModel',
						'dataset' => $dataSet['title'],
						'createdTime' => date('c', $createdTime),
						'parameters' => array(
							'pid' => $pid,
							'tableId' => $dataSet['tableId']
						),
						'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
					);
					$jobInfo = $this->createJob($jobData);

					$definitionUrl = $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $dataSet['tableId']), json_encode($dataSet['definition']));
					$this->sharedConfig->saveJob($jobInfo['id'], array(
						'definition' => $definitionUrl
					));
				}


				$jobData = array(
					'batchId' => $batchId,
					'runId' => $runId,
					'command' => 'loadData',
					'dataset' => $dataSet['title'],
					'createdTime' => date('c', $createdTime),
					'parameters' => array(
						'pid' => $pid,
						'tableId' => $dataSet['tableId']
					),
					'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
				);
				if (isset($this->params['incrementalLoad'])) {
					$jobData['parameters']['incrementalLoad'] = $this->params['incrementalLoad'];
				}
				$jobInfo = $this->createJob($jobData);

				$definitionUrl = $this->getS3Client()->uploadString(sprintf('%s/%s.json', $jobInfo['id'], $dataSet['tableId']), json_encode($dataSet['definition']));
				$this->sharedConfig->saveJob($jobInfo['id'], array(
					'definition' => $definitionUrl
				));
			}
		}

		// Execute reports
		foreach ($projectsToUse as $pid) {
			$jobData = array(
				'batchId' => $batchId,
				'runId' => $runId,
				'command' => 'executeReports',
				'createdTime' => date('c', $createdTime),
				'parameters' => array(
					'pid' => $pid
				),
				'queue' => isset($this->params['queue']) ? $this->params['queue'] : null
			);
			$this->createJob($jobData);
		}

		$this->enqueue($batchId);


		return $this->getPollResult($batchId, $this->params['writerId'], true);
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

		$tableConfiguration = $this->getConfiguration()->getDataSet($this->params['tableId']);
		$jobInfo = $this->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'dataset' => !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'],
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
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}

		if (!$project['active']) {
			throw new WrongParametersException($this->translator->trans('configuration.project.not_active %1', array('%1' => $this->params['pid'])));
		}

		$reports = array();
		if (!empty($this->params['reports'])) {
			$reports = (array) $this->params['reports'];

			foreach ($reports AS $reportLink) {
				if (!preg_match('/^\/gdc\/md\/' . $this->params['pid'] . '\//', $reportLink)) {
					throw new WrongParametersException($this->translator->trans('parameters.report.not_valid %1', array('%1' => $reportLink)));
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

		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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

		return $this->getPollResult($jobInfo['id'], $this->params['writerId']);
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
			throw new WrongParametersException($this->translator->trans('parameters.tableId'));
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
			if (!empty($dimensions[$this->params['name']]['isExported'])) {
				throw new WrongParametersException($this->translator->trans('error.dimension_uploaded'));
			}
			$this->getConfiguration()->deleteDateDimension($this->params['name']);
			return $this->createApiResponse();
		} else {
			throw new WrongParametersException($this->translator->trans('parameters.dimension_name'));
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
		$template = !empty($this->params['template'])? $this->params['template'] : null;

		$dimensions = $this->getConfiguration()->getDateDimensions();
		if (!isset($dimensions[$this->params['name']])) {
			$this->getConfiguration()->saveDateDimension($this->params['name'], !empty($this->params['includeTime']), $template);
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
				throw new WrongParametersException($this->translator->trans('parameters.jobId_number'));
			}
			$job = $this->getSharedConfig()->fetchJob($this->params['jobId'], $this->getConfiguration()->writerId, $this->getConfiguration()->projectId);
			if (!$job) {
				throw new WrongParametersException($this->translator->trans('parameters.job'));
			}

			$job = $this->getSharedConfig()->jobToApiResponse($job, $this->getS3Client());
			return $this->createJsonResponse($job);
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

		$batch = $this->getSharedConfig()->batchToApiResponse($this->params['batchId'], $this->getS3Client());
		return $this->createJsonResponse($batch);
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
		$responseBody = array(
			'status'    => 'ok'
		);

		if ($this->stopWatch->isStarted(self::STOPWATCH_NAME_REQUEST)) {
			$event = $this->stopWatch->stop(self::STOPWATCH_NAME_REQUEST);
			$responseBody['duration']  = $event->getDuration();
		}

		if (null != $response) {
			$responseBody = array_merge($response, $responseBody);
		}

		return $this->createJsonResponse($responseBody, $statusCode);
	}

	private function getPollResult($id, $writerId, $isBatch = false, $jobId=null)
	{
		/** @var \Symfony\Component\Routing\RequestContext $context */
		$context = $this->container->get('router')->getContext();

		$result = array(
			($isBatch? 'batch' : 'job') => (int)$id,
			'url' => sprintf('https://%s%s/gooddata-writer/%s?writerId=%s&%s=%s', $context->getHost(), $context->getBaseUrl(),
				$isBatch? 'batch' : 'jobs', $writerId, $isBatch? 'batchId' : 'jobId', $id)
		);
		if ($jobId) $result['job'] = (int)$jobId;
		return $this->createApiResponse($result, 202);
	}

	private function getConfiguration()
	{
		if (!isset($this->params['writerId'])) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.required'));
		}
		if (!$this->configuration) {
			$this->configuration = new Configuration($this->storageApi, $this->getSharedConfig());
			$this->configuration->setWriterId($this->params['writerId']);
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
				$this->getConfiguration()->projectId . '.' . $this->getConfiguration()->writerId,
				$this->container->get('logger')
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
				throw new WrongParametersException($this->translator->trans('parameters.required %1', array('%1' => $k)));
			}
		}
	}

	private function checkWriterExistence()
	{
		$tokenInfo = $this->storageApi->getLogData();
		$projectId = $tokenInfo['owner']['id'];

		if (!$this->getSharedConfig()->writerExists($projectId, $this->params['writerId'])) {
			$bucket = $this->getConfiguration()->findConfigurationBucket($this->params['writerId']);
			if ($bucket) {
				$this->getSharedConfig()->setWriterStatus($projectId, $this->params['writerId'], SharedConfig::WRITER_STATUS_READY);
			} else {
				throw new WrongParametersException($this->translator->trans('parameters.writerId.not_found'));
			}
		}
	}

	private function createJob($params)
	{
		$tokenData = $this->storageApi->getLogData();
		$job = $this->getSharedConfig()->createJob($this->getConfiguration()->projectId, $this->getConfiguration()->writerId,
			$this->storageApi->getRunId(), $this->storageApi->token, $tokenData['id'], $tokenData['description'], $params);

		$inputParams = isset($params['parameters'])? $params['parameters'] : array();
		array_walk($inputParams, function(&$val, $key) {
			if ($key == 'password') $val = '***';
		});
		$this->container->get('logger')->log(Logger::INFO, $this->translator->trans('log.job.created %1', array('%1' => $job['id'])), array(
			'projectId' => $this->getConfiguration()->projectId,
			'writerId' => $this->getConfiguration()->writerId,
			'runId' => $this->storageApi->getRunId(),
			'command' => $params['command'],
			'params' => $inputParams
		));

		return $job;
	}

	/**
	 * @param $batchId
	 */
	protected function enqueue($batchId, $otherData = array())
	{
		$data = array(
			'projectId' => $this->getConfiguration()->projectId,
			'writerId' => $this->getConfiguration()->writerId,
			'batchId' => $batchId
		);
		if (count($otherData)) $data = array_merge($data, $otherData);
		$this->getWriterQueue()->enqueue($data);
	}



	protected function getProjectsToUse()
	{
		$this->configuration->checkProjectsTable();
		$projects = array();
		foreach ($this->configuration->getProjects() as $project) if ($project['active']) {
			if (in_array($project['pid'], $projects)) {
				throw new WrongConfigurationException($this->translator->trans('configuration.project.duplicated %1', array('%1' => $project['pid'])));
			}
			if (!isset($this->params['pid']) || $project['pid'] == $this->params['pid']) {
				$projects[] = $project['pid'];
			}
		}
		if (isset($params['pid']) && !count($projects)) {
			throw new WrongConfigurationException($this->translator->trans('parameters.pid_not_configured'));
		}
		return $projects;
	}

}
