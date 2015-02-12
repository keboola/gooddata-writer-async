<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-03-14
 */

namespace Keboola\GoodDataWriter\Controller;


use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Job\JobFactory;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Writer\SharedStorageException;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Method;

use Guzzle\Http\Url,
	Guzzle\Common\Exception\InvalidArgumentException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response,
	Symfony\Component\HttpKernel\Exception\HttpException,
	Symfony\Component\Stopwatch\Stopwatch;

use Syrup\ComponentBundle\Exception\SyrupComponentException;
use Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\SSO,
	Keboola\GoodDataWriter\Model\Graph,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedStorage;
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
	 * @var SharedStorage
	 */
	public $sharedStorage;

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

	private $projectId;
	private $writerId;
	private $paramQueue;

	/**
	 * @var JobFactory
	 */
	private $jobFactory;
    /**
     * @var S3Client
     */
    private $s3Client;

	/**
	 * Common things to do for each request
	 */
	public function preExecute(Request $request)
	{
		parent::preExecute($request);

		$this->translator = $this->container->get('translator');

		set_time_limit(3 * 60 * 60);

		// Get params
		$this->method = $request->getMethod();
		$this->params = in_array($this->method, array('POST', 'PUT'))? $this->getPostJson($request) : $request->query->all();
		array_walk_recursive($this->params, function(&$param) {
			$param = trim($param);
		});

		if (isset($this->params['queue']) && !in_array($this->params['queue'], array(SharedStorage::PRIMARY_QUEUE, SharedStorage::SECONDARY_QUEUE))) {
			throw new WrongParametersException($this->translator->trans('parameters.queue %1',
				array('%1' => SharedStorage::PRIMARY_QUEUE . ', ' . SharedStorage::SECONDARY_QUEUE)));
		}
		$this->paramQueue = isset($this->params['queue'])? $this->params['queue'] : SharedStorage::PRIMARY_QUEUE;

		$tokenInfo = $this->storageApi->getLogData();
		$this->projectId = $tokenInfo['owner']['id'];
		$this->writerId = empty($this->params['writerId'])? null : $this->params['writerId'];


        $this->s3Client = $this->container->get('gooddata_writer.s3Client');
		$this->eventLogger = new EventLogger($this->storageApi, $this->s3Client);

		$this->stopWatch = new Stopwatch();
		$this->stopWatch->start(self::STOPWATCH_NAME_REQUEST);
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
		$jobName = 'optimizeSliHash';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, SharedStorage::SERVICE_QUEUE);

		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}


	/**
	 * Create writer
	 *
	 * @Route("/writers")
	 * @Method({"POST"})
	 */
	public function postWritersAction()
	{
		if (!$this->writerId) {
			throw new WrongConfigurationException($this->translator->trans('parameters.required %1', array('%1' => 'writerId')));
		}
		if ($this->getSharedStorage()->writerExists($this->projectId, $this->writerId)) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.exists'));
		}

		$jobName = 'createWriter';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		try {
			/** @var RestApi $restApi */
			$restApi = $this->container->get('gooddata_writer.rest_api');
			if (!$restApi->ping()) {
				return $this->createMaintenanceResponse();
			}
			try {
				$bucketAttributes = $this->getConfiguration()->bucketAttributes();
				if (!empty($bucketAttributes['gd']['apiUrl'])) {
					$restApi->setBaseUrl($bucketAttributes['gd']['apiUrl']);
				}
			} catch (WrongConfigurationException $e) {
				// ignore non-existing config bucket
			}
			$params = $command->prepare($this->params, $restApi);
		} catch (RestApiException $e) {
			$e = new JobProcessException($e->getMessage(), $e);
			$e->setData($e->getData());
			throw $e;
		}

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, SharedStorage::SERVICE_QUEUE);

		if(!empty($params['users'])) foreach ($params['users'] as $user) {
			$this->getJobFactory()->createJob('addUserToProject', array('email' => $user, 'role' => 'admin'), $batchId,
				SharedStorage::SERVICE_QUEUE, array('dataset' => $user));
		}

		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}

	/**
	 * Update writer attributes
	 *
	 * @Route("/writers/{writerId}")
	 * @Method({"POST"})
	 */
	public function updateWriterAction($writerId)
	{
		$this->writerId = $writerId;

		$this->checkWriterExistence();

		// Update writer configuration
		$reservedAttrs = array('id', 'bucket', 'status', 'info', 'created');
		foreach ($this->params as $key => $value) if ($key != 'writerId') {
			if (in_array($key, $reservedAttrs)) {
				throw new WrongParametersException($this->translator->trans('parameters.writer_attr %1', array('%1' => implode(', ', $reservedAttrs))));
			}
			if (is_array($value)) $value = json_encode($value);
			$this->getConfiguration()->updateBucketAttribute($key, $value);
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
		$jobName = 'deleteWriter';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, SharedStorage::SERVICE_QUEUE);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}

	/**
	 * Delete configuration
	 * alias for deleteWritersAction()
	 *
	 * @Route("/configs/{configId}")
	 * @Method({"DELETE"})
	 */
	public function deleteConfigsAction($configId)
	{
		$this->params['writerId'] = $configId;
		$this->writerId = $configId;
		return $this->deleteWritersAction();
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
		if ($writerId) {
			$this->writerId = $writerId;
		}

		if ($this->writerId) {
			$this->checkWriterExistence();
			return $this->createApiResponse(array(
				'writer' => $this->getConfiguration()->getWriterToApi()
			));
		} else {
			$configuration = new Configuration($this->storageApi, $this->getSharedStorage());
			return $this->createApiResponse(array(
				'writers' => $configuration->getWritersToApi()
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
		$jobName = 'cloneProject';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
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
		$jobName = 'addUserToProject';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue, array('dataset' => $params['email']));
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}

	/**
	 * Delete project users
	 *
	 * @Route("/project-users")
	 * @Method({"DELETE"})
	 */
	public function deleteProjectUsersAction()
	{
		$jobName = 'removeUserFromProject';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue, array('dataset' => $params['email']));
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
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
		$jobName = 'createUser';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue, array('dataset' => $params['email']));
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
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
		/** @var RestApi $restApi */
		$restApi = $this->container->get('gooddata_writer.rest_api');
		if (!$restApi->ping()) {
			return $this->createMaintenanceResponse();
		}

		// Init parameters
		$this->checkParams(array('writerId', 'email', 'pid'));
		$this->checkWriterExistence();

		if (!$this->getSharedStorage()->projectBelongsToWriter($this->projectId, $this->writerId, $this->params['pid'])) {
			throw new WrongParametersException($this->translator->trans('parameters.sso_wrong_pid'));
		}

		if (!empty($this->params['createUser']) && $this->params['createUser'] == 1 && !$this->getConfiguration()->getUser($this->params['email'])) {
			$result = $this->postUsersAction();
			$jsonResult = json_decode($result->getContent(), true);
			$jobFinished = false;
			$i = 1;
			do {
				$job = $this->getSharedStorage()->fetchJob($jsonResult['job'], $this->projectId, $this->writerId);
				if (!$job) {
					throw new WrongParametersException(sprintf("Job '%d' not found", $this->params['jobId']));
				}
				$jobInfo = SharedStorage::jobToApiResponse($job, $this->s3Client);
				if (isset($jobInfo['status']) && SharedStorage::isJobFinished($jobInfo['status'])) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep($i * 5);
				$i++;
			} while(!$jobFinished);

			if ($jobInfo['status'] == SharedStorage::JOB_STATUS_SUCCESS) {
				if (!empty($jobInfo['result']['alreadyExists'])) {
					throw new JobProcessException($this->translator->trans('result.cancelled'));
				}
				// Do nothing
			} elseif ($jobInfo['status'] == SharedStorage::JOB_STATUS_CANCELLED) {
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
				$job = $this->getSharedStorage()->fetchJob($jsonResult['job'], $this->projectId, $this->writerId);
				if (!$job) {
					throw new WrongParametersException(sprintf("Job '%d' not found", $this->params['jobId']));
				}
				$jobInfo = SharedStorage::jobToApiResponse($job, $this->s3Client);
				if (isset($jobInfo['status']) && SharedStorage::isJobFinished($jobInfo['status'])) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep($i * 5);
				$i++;
			} while(!$jobFinished);

			if ($jobInfo['status'] == SharedStorage::JOB_STATUS_SUCCESS) {
				// Do nothing
			} elseif ($jobInfo['status'] == SharedStorage::JOB_STATUS_CANCELLED) {
				throw new JobProcessException($this->translator->trans('result.cancelled'));
			} else {
				$e = new JobProcessException(!empty($jobInfo['result']['error'])? $jobInfo['result']['error'] : $this->translator->trans('result.unknown'));
				$e->setData(array('result' => $jobInfo['result'], 'logs' => $jobInfo['logs']));
				throw $e;
			}
		}

		if (!$this->getSharedStorage()->userBelongsToWriter($this->projectId, $this->writerId, $this->params['email'])) {
			throw new WrongParametersException($this->translator->trans('parameters.sso_wrong_email'));
		}

		$targetUrl = isset($this->params['targetUrl'])? $this->params['targetUrl'] : '/#s=/gdc/projects/' . $this->params['pid'];
		$validity = (isset($this->params['validity']))? $this->params['validity'] : 86400;

		$gdConfig = $config = $this->container->getParameter('gdwr_gd');
		if (!isset($gdConfig['domain'])) {
			throw new \Exception("Key 'domain' is missing from gd config");
		}
		$domainUser = $this->getSharedStorage()->getDomainUser($gdConfig['domain']);

		$ssoProvider = isset($gdConfig['sso_provider'])? $gdConfig['sso_provider'] : Model::SSO_PROVIDER;
		$passphrase = $this->container->getParameter('gdwr_key_passphrase');
		$ssoLink = SSO::url($domainUser->username, $ssoProvider, $passphrase, $this->container->get('syrup.temp'), $targetUrl, $this->params['email'], $validity);

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
		$jobName = 'proxyCall';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
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
		if (!$restApi->ping()) {
			return $this->createMaintenanceResponse();
		}

		$bucketAttributes = $this->getConfiguration()->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		if (!empty($bucketAttributes['gd']['apiUrl'])) {
			$restApi->setBaseUrl($bucketAttributes['gd']['apiUrl']);
		}

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
		$jobName = 'createFilter';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}

	/**
	 * Delete user filter
	 *
	 * @Route("/filters")
	 * @Method({"DELETE"})
	 */
	public function deleteFiltersAction()
	{
		$jobName = 'deleteFilter';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
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
		$jobName = 'assignFiltersToUser';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}

	/**
	 * Synchronize filters from writer's configuration to GoodData project
	 *
	 * @Route("/sync-filters")
	 * @Method({"POST"})
	 */
	public function postSyncFiltersAction()
	{
		$jobName = 'syncFilters';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = false;
		$projects = empty($params['pid'])? $this->getProjectsToUse() : array($params['pid']);
		foreach ($projects as $pid) {
			$job = $this->getJobFactory()->createJob($jobName, array('pid' => $pid), $batchId, $this->paramQueue);
		}
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job? $job['id'] : null);
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
		$jobName = 'UploadDateDimension';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = false;
		foreach ($this->getProjectsToUse() as $pid) {
			$pars = array('pid' => $pid, 'name' => $params['name'], 'includeTime' => $params['includeTime']);
			$job = $this->getJobFactory()->createJob($jobName, $pars, $batchId, $this->paramQueue, array('dataset' => $pars['name']));
		}

		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job? $job['id'] : null);
	}

	/**
	 * @Route("/update-model")
	 * @Method({"POST"})
	 * params: pid, queue
	 */
	public function postUpdateModel()
	{
		$jobName = 'updateModel';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = false;
		$definition = $this->getConfiguration()->getDataSetDefinition($params['tableId']);
		$tableConfiguration = $this->getConfiguration()->getDataSet($params['tableId']);
		foreach ($this->getProjectsToUse() as $pid) {
			$datasetName = !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'];
			$job = $this->getJobFactory()->createJob($jobName, array('pid' => $pid, 'tableId' => $params['tableId']),
				$batchId, $this->paramQueue, array('dataset' => $datasetName));
			$this->getJobFactory()->saveDefinition($job['id'], $definition);
		}

		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job? $job['id'] : null);
	}

	/**
	 * @Route("/load-data")
	 * @Method({"POST"})
	 * params: pid, queue
	 */
	public function postLoadData()
	{
		$jobName = 'loadData';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = false;
		foreach ($params['tables'] as $tableId) {
			$definition = $this->getConfiguration()->getDataSetDefinition($tableId);
			$tableConfiguration = $this->getConfiguration()->getDataSet($tableId);
			foreach ($this->getProjectsToUse() as $pid) {
				$loadParams = array('pid' => $pid, 'tableId' => $tableId);
				if (isset($params['incrementalLoad'])) {
					$loadParams['incrementalLoad'] = $params['incrementalLoad'];
				}
				$datasetName = !empty($tableConfiguration['name']) ? $tableConfiguration['name'] : $tableConfiguration['id'];
				$job = $this->getJobFactory()->createJob($jobName, $loadParams, $batchId, $this->paramQueue, array('dataset' => $datasetName));
				$this->getJobFactory()->saveDefinition($job['id'], $definition);
			}
		}

		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job? $job['id'] : null);
	}

	/**
	 * @Route("/load-data-multi")
	 * @Method({"POST"})
	 * params: pid, queue
	 */
	public function postLoadDataMulti()
	{
		$jobName = 'loadDataMulti';
		$command = $this->getJobFactory()->getJobClass($jobName);

		$params = $command->prepare($this->params);

		$job = false;
		$batchId = $this->getJobFactory()->createBatchId();
		foreach ($this->getProjectsToUse() as $pid) {
			$definition = array();
			foreach ($params['tables'] as $tableId) {
				$definition[$tableId] = array(
					'columns' => $this->getConfiguration()->getDataSetDefinition($tableId),
					'dataset' => $this->getConfiguration()->getDataSet($tableId)
				);
			}

			$loadParams = array('pid' => $pid, 'tables' => $params['tables']);
			if (isset($params['incrementalLoad'])) {
				$loadParams['incrementalLoad'] = $params['incrementalLoad'];
			}
			$job = $this->getJobFactory()->createJob($jobName, $loadParams, $batchId, $this->paramQueue);
			$this->getJobFactory()->saveDefinition($job['id'], $definition);
		}

		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job? $job['id'] : null);
	}


	/**
	 * Upload dataSet to GoodData
	 *
	 * @Route("/upload-table")
	 * @Method({"POST"})
	 */
	public function postUploadTableAction()
	{
		// Init parameters
		$this->checkParams(array('writerId', 'tableId'));
		$this->checkWriterExistence();

		$bucketAttributes = $this->getConfiguration()->bucketAttributes();

		$batchId = $this->getJobFactory()->createBatchId();
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
					$params = array(
						'pid' => $pid,
						'name' => $dimension,
						'includeTime' => $dateDimensions[$dimension]['includeTime']
					);
					$this->getJobFactory()->createJob('uploadDateDimension', $params, $batchId, $this->paramQueue,
						array('dataset' => $dimension));
				}
			}
		}

		try {
			/** @var RestApi $restApi */
			$restApi = $this->container->get('gooddata_writer.rest_api');
			if (!$restApi->ping()) {
				return $this->createMaintenanceResponse();
			}

			$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
			if (!empty($bucketAttributes['gd']['apiUrl'])) {
				$restApi->setBaseUrl($bucketAttributes['gd']['apiUrl']);
			}

			$jobData = null;
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
					$params = array(
						'pid' => $pid,
						'tableId' => $this->params['tableId']
					);
					$jobData = $this->getJobFactory()->createJob('updateModel', $params, $batchId, $this->paramQueue,
						array('dataset' => $dataSetName));
					$this->getJobFactory()->saveDefinition($jobData['id'], $definition);
				}

				$params = array(
					'pid' => $pid,
					'tableId' => $this->params['tableId']
				);
				if (isset($this->params['incrementalLoad'])) {
					$params['incrementalLoad'] = $this->params['incrementalLoad'];
				}
				$jobData = $this->getJobFactory()->createJob('loadData', $params, $batchId, $this->paramQueue,
					array('dataset' => $dataSetName));
				$this->getJobFactory()->saveDefinition($jobData['id'], $definition);
			}
		} catch (RestApiException $e) {
			$e = new JobProcessException($e->getMessage(), $e);
			$e->setData($e->getData());
			throw $e;
		}

		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $jobData? $jobData['id'] : null);
	}

	/**
	 * Upload project to GoodData
	 *
	 * @Route("/upload-project")
	 * @Method({"POST"})
	 */
	public function postUploadProjectAction()
	{
		$this->checkParams(array('writerId'));
		$this->checkWriterExistence();

		$bucketAttributes = $this->getConfiguration()->bucketAttributes();
		$projectsToUse = $this->getProjectsToUse();

		$this->getConfiguration()->getDateDimensions();
		$batchId = $this->getJobFactory()->createBatchId();

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
					throw new WrongParametersException($this->translator->trans('configuration.dimension_not_found %d %c %t',
						array('%d' => $dimension, '%c' => $column['name'], '%t' => $dataSet['tableId'])));
				}

				if (!$dateDimensions[$dimension]['isExported'] && !in_array($dimension, $dateDimensionsToLoad)) {
					$dateDimensionsToLoad[] = $dimension;

					foreach ($projectsToUse as $pid) {
						$params = array(
							'pid' => $pid,
							'name' => $dimension,
							'includeTime' => $dateDimensions[$dimension]['includeTime']
						);
						$this->getJobFactory()->createJob('uploadDateDimension', $params, $batchId, $this->paramQueue,
							array('dataset' => $dimension));
					}
				}
			}
		}


		try {
			/** @var RestApi $restApi */
			$restApi = $this->container->get('gooddata_writer.rest_api');
			if (!$restApi->ping()) {
				return $this->createMaintenanceResponse();
			}
			$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
			if (!empty($bucketAttributes['gd']['apiUrl'])) {
				$restApi->setBaseUrl($bucketAttributes['gd']['apiUrl']);
			}
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
						$params = array(
							'pid' => $pid,
							'tableId' => $dataSet['tableId']
						);
						$jobData = $this->getJobFactory()->createJob('updateModel', $params, $batchId, $this->paramQueue,
							array('dataset' => $dataSet['title']));
						$this->getJobFactory()->saveDefinition($jobData['id'], $dataSet['definition']);
					}

					$params = array(
						'pid' => $pid,
						'tableId' => $dataSet['tableId']
					);
					if (isset($this->params['incrementalLoad'])) {
						$params['incrementalLoad'] = $this->params['incrementalLoad'];
					}
					$jobData = $this->getJobFactory()->createJob('loadData', $params, $batchId, $this->paramQueue,
						array('dataset' => $dataSet['title']));
					$this->getJobFactory()->saveDefinition($jobData['id'], $dataSet['definition']);
				}
			}
		} catch (RestApiException $e) {
			$e = new JobProcessException($e->getMessage(), $e);
			$e->setData($e->getData());
			throw $e;
		}

		// Execute reports
		$job = null;
		foreach ($projectsToUse as $pid) {
			$job = $this->getJobFactory()->createJob('executeReports', array('pid' => $pid), $batchId, $this->paramQueue);
		}

		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job? $job['id'] : null);
	}

	/**
	 * Reset dataSet and remove it from GoodData project
	 *
	 * @Route("/reset-table")
	 * @Method({"POST"})
	 */
	public function postResetTableAction()
	{
		$jobName = 'resetTable';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}


	/**
	 * Reset GoodData project
	 *
	 * @Route("/reset-project")
	 * @Method({"POST"})
	 */
	public function postResetProjectAction()
	{
		$jobName = 'resetProject';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}

	/**
	 * Execute Reports in GoodData
	 *
	 * @Route("/execute-reports")
	 * @Method({"POST"})
	 */
	public function postExecuteReportsAction()
	{
		$jobName = 'executeReports';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
	}


	/**
	 * Export report data from GoodData
	 *
	 * @Route("/export-report")
	 * @Method({"POST"})
	 */
	public function postExportReportAction()
	{
		$jobName = 'exportReport';
		$command = $this->getJobFactory()->getJobClass($jobName, $this->params);

		$batchId = $this->getJobFactory()->createBatchId();
		$params = $command->prepare($this->params);

		$job = $this->getJobFactory()->createJob($jobName, $params, $batchId, $this->paramQueue);
		$this->getJobFactory()->enqueueJob($batchId);
		return $this->createPollResponse($batchId, $this->writerId, $job['id']);
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
			$this->container->getParameter('storage_api.url'), $this->projectId, $this->writerId);
		$tableUrl = sprintf('%s/admin/projects-new/%s/gooddata?config=%s#/table/',
			$this->container->getParameter('storage_api.url'), $this->projectId, $this->writerId);
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
			$days = isset($this->params['days']) ? $this->params['days'] : 3;
			$tableId = empty($this->params['tableId']) ? null : $this->params['tableId'];
			$command = empty($this->params['command']) ? null : $this->params['command'];
			$tokenId = empty($this->params['tokenId']) ? null : $this->params['tokenId'];
			$status = empty($this->params['status']) ? null : $this->params['status'];
			$jobs = $this->getSharedStorage()->fetchJobs($this->projectId, $this->writerId, $days, $tableId);

			$result = array();
			foreach ($jobs as $job) {
				if ((empty($command) || $command == $job['command']) && (empty($tokenId) || $tokenId == $job['tokenId'])
					&& (empty($status) || $status == $job['status'])) {
					if (empty($tableId) || (!empty($job['parameters']['tableId']) && $job['parameters']['tableId'] == $tableId)) {
						$result[] = SharedStorage::jobToApiResponse($job, $this->s3Client);
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
			$job = $this->getSharedStorage()->fetchJob($this->params['jobId'], $this->projectId, $this->writerId);
			if (!$job) {
				throw new WrongParametersException($this->translator->trans('parameters.job'));
			}

			$job = SharedStorage::jobToApiResponse($job, $this->s3Client);

			$this->logApiCall();
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

		$jobs = $this->getSharedStorage()->fetchBatch($this->params['batchId']);
		if (!count($jobs)) {
			throw new WrongParametersException(sprintf("Batch '%d' not found", $this->params['batchId']));
		}
		$batch = SharedStorage::batchToApiResponse($this->params['batchId'], $jobs);

		$this->logApiCall();
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

        $this->getSharedStorage()->cancelJobs($this->projectId, $this->writerId);
        return $this->createApiResponse();
    }

    /**
     * Run method is not supported
     *
     * @Route("/run")
     * @Method({"POST"})
     */
    public function runAction(Request $request)
    {
        return $this->createApiResponse([], 405);
    }



	/***************************************************************************
	 * *************************************************************************
	 * @section Helpers
	 */

	private function createApiResponse($response=array(), $statusCode=200, $statusMessage=null)
	{
		if (!$statusMessage) {
			$statusMessage = ($statusCode >= 300)? 'error' : 'ok';
		}
		$responseBody = array(
			'status' => $statusMessage
		);

		if ($this->stopWatch->isStarted(self::STOPWATCH_NAME_REQUEST)) {
			$event = $this->stopWatch->stop(self::STOPWATCH_NAME_REQUEST);
			$responseBody['duration']  = $event->getDuration();
		}

		$this->logApiCall(isset($responseBody['duration'])? $responseBody['duration'] : null);

		if (null != $response) {
			$responseBody = array_merge($response, $responseBody);
		}

		return $this->createJsonResponse($responseBody, $statusCode);
	}

	public function createMaintenanceResponse() {
		return $this->createApiResponse(array(
			'error' => 'There is undergoing maintenance on GoodData backend, please try again later.'
		), 503, 'maintenance');
	}

	private function createPollResponse($batchId, $writerId, $jobId=null, $responseCode=202)
	{
		/** @var \Symfony\Component\Routing\RequestContext $context */
		$context = $this->container->get('router')->getContext();

		$result = array(
			'batch' => (int)$batchId,
			'url' => sprintf('https://%s%s/gooddata-writer/batch?writerId=%s&batchId=%s',
				$context->getHost(), $context->getBaseUrl(), $writerId, $batchId)
		);
		if ($jobId) $result['job'] = (int)$jobId;
		return $this->createApiResponse($result, $responseCode);
	}

	private function getConfiguration()
	{
		if (!$this->writerId) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.required'));
		}
		if (!$this->configuration) {
			$this->configuration = new Configuration($this->storageApi, $this->getSharedStorage());
			$this->configuration->setWriterId($this->writerId);
		}
		return $this->configuration;
	}

	private function getSharedStorage()
	{
		if (!$this->sharedStorage) {
			$this->sharedStorage = $this->container->get('gooddata_writer.shared_storage');
		}
		return $this->sharedStorage;
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
		if (!$this->getSharedStorage()->writerExists($this->projectId, $this->writerId)) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.not_found'));
		}
	}



	protected function getProjectsToUse()
	{
		$this->configuration->checkProjectsTable();
		$projects = array();
		foreach ($this->getConfiguration()->getProjects() as $project) if ($project['active']) {
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

	protected function getJobFactory()
	{
		if (!$this->jobFactory) {
			$this->jobFactory = new JobFactory(
				$this->container->getParameter('gdwr_gd'),
				$this->getSharedStorage(),
				$this->getConfiguration(),
				$this->storageApi,
				$this->container->getParameter('gdwr_scripts_path'),
				$this->eventLogger,
				$this->translator,
				$this->temp,
				$this->logger,
				$this->container->get('gooddata_writer.s3Client'),
				$this->container->get('gooddata_writer.jobs_queue')
			);
		}
		return $this->jobFactory;
	}
	

	public function logApiCall($duration=null)
	{
		$params = array();
		if (is_array($this->params)) foreach ($this->params as $k => $p) {
			$params[$k] = ($k == 'password')? '***' : $p;
		}
		/** @var \Symfony\Bundle\FrameworkBundle\Routing\Router $router */
		$router = $this->get('router');
		if ($this->eventLogger) {
			try {
				@$this->eventLogger->log(
					$this->writerId,
					$this->storageApi->getRunId(),
					'Called API ' . $router->getContext()->getMethod() . ' ' . $router->getContext()->getPathInfo(),
					$params,
					$duration
				);
			} catch (\Exception $e) {
				// Ignore
			}
		}
	}

}
