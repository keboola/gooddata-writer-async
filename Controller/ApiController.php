<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-03-14
 */

namespace Keboola\GoodDataWriter\Controller;

use Guzzle\Http\Url;
use Guzzle\Common\Exception\InvalidArgumentException;
use Keboola\GoodDataWriter\Elasticsearch\Search;
use Keboola\GoodDataWriter\StorageApi\CachedClient;
use Keboola\GoodDataWriter\Task\Factory;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\Syrup\Exception\ApplicationException;
use Keboola\Syrup\Exception\UserException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Symfony\Component\Stopwatch\Stopwatch;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Method;
use Keboola\Syrup\Exception\SyrupComponentException;
use Keboola\Temp\Temp;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\SSO;
use Keboola\GoodDataWriter\Model\Graph;
use Keboola\GoodDataWriter\StorageApi\EventLogger;
use Keboola\GoodDataWriter\Aws\S3Client;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;

use Keboola\GoodDataWriter\Exception\GraphTtlException;

class ApiController extends \Keboola\Syrup\Controller\ApiController
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
     * @var \Keboola\GoodDataWriter\StorageApi\EventLogger
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
     * @var \Keboola\GoodDataWriter\Job\Metadata\JobFactory
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
        $this->params = in_array($this->method, ['POST', 'PUT']) ? $this->getPostJson($request) : $request->query->all();
        array_walk_recursive($this->params, function (&$param) {
            $param = trim($param);
        });

        if (isset($this->params['queue']) && !in_array($this->params['queue'], [Job::PRIMARY_QUEUE, Job::SECONDARY_QUEUE])) {
            throw new UserException($this->translator->trans(
                'parameters.queue %1',
                ['%1' => Job::PRIMARY_QUEUE . ', ' . Job::SECONDARY_QUEUE]
            ));
        }
        $this->paramQueue = isset($this->params['queue']) ? $this->params['queue'] : Job::PRIMARY_QUEUE;

        $tokenInfo = $this->storageApi->getLogData();
        $this->projectId = $tokenInfo['owner']['id'];
        $this->writerId = empty($this->params['writerId']) ? null : $this->params['writerId'];


        $this->s3Client = $this->container->get('gooddata_writer.s3_client');
        $this->eventLogger = new EventLogger($this->storageApi, $this->s3Client);

        $this->stopWatch = new Stopwatch();
        $this->stopWatch->start(self::STOPWATCH_NAME_REQUEST);
    }

    private function getJob($queue = null)
    {
        return $this->getJobFactory()->create($queue ?: $this->paramQueue);
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
        $taskName = 'optimizeSliHash';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob(Job::SERVICE_QUEUE);
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
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
            throw new UserException($this->translator->trans('parameters.required %1', ['%1' => 'writerId']));
        }
        if ($this->getSharedStorage()->writerExists($this->projectId, $this->writerId)) {
            throw new UserException($this->translator->trans('parameters.writerId.exists'));
        }

        $job = $this->getJob(Job::SERVICE_QUEUE);

        $taskName = 'createWriter';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job->addTask($taskName, $params);

        if (!empty($params['users'])) {
            foreach ($params['users'] as $user) {
                $job->addTask('addUserToProject', ['email' => $user, 'role' => 'admin']);
            }
        }

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
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
        $reservedAttrs = ['id', 'bucket', 'status', 'info', 'created'];
        foreach ($this->params as $key => $value) {
            if ($key != 'writerId') {
                if (in_array($key, $reservedAttrs)) {
                    throw new UserException($this->translator->trans('parameters.writer_attr %1', ['%1' => implode(', ', $reservedAttrs)]));
                }
                if (is_array($value)) {
                    $value = json_encode($value);
                }
                $this->getConfiguration()->updateBucketAttribute($key, $value);
            }
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
        $taskName = 'deleteWriter';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob(Job::SERVICE_QUEUE);
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
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
    public function getWritersAction($writerId = null)
    {
        if ($writerId) {
            $this->writerId = $writerId;
        }

        if ($this->writerId) {
            $this->checkWriterExistence();
            return $this->createApiResponse([
                'writer' => $this->getConfiguration()->getWriterToApi()
            ]);
        } else {
            $configuration = new Configuration(new CachedClient($this->storageApi), $this->getSharedStorage());
            return $this->createApiResponse([
                'writers' => $configuration->getWritersToApi()
            ]);
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
        $taskName = 'cloneProject';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * List cloned projects
     *
     * @Route("/projects")
     * @Method({"GET"})
     */
    public function getProjectsAction()
    {
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        return $this->createApiResponse([
            'projects' => $this->getConfiguration()->getProjects()
        ]);
    }


    /**
     * Create project users
     *
     * @Route("/project-users")
     * @Method({"POST"})
     */
    public function postProjectUsersAction()
    {
        $taskName = 'addUserToProject';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * Delete project users
     *
     * @Route("/project-users")
     * @Method({"DELETE"})
     */
    public function deleteProjectUsersAction()
    {
        $taskName = 'removeUserFromProject';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * List project users
     *
     * @Route("/project-users")
     * @Method({"GET"})
     */
    public function getProjectUsersAction()
    {
        $this->checkParams(['writerId', 'pid']);
        $this->checkWriterExistence();

        return $this->createApiResponse([
            'users' => $this->getConfiguration()->getProjectUsers($this->params['pid'])
        ]);
    }


    /**
     * Create users
     *
     * @Route("/users")
     * @Method({"POST"})
     */
    public function postUsersAction()
    {
        $taskName = 'createUser';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * List users
     *
     * @Route("/users")
     * @Method({"GET"})
     */
    public function getUsersAction()
    {
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        if (isset($this->params['userEmail'])) {
            $user = $this->getConfiguration()->getUser($this->params['userEmail']);
            return $this->createApiResponse([
                'user' => $user ? $user : null
            ]);
        } else {
            return $this->createApiResponse([
                'users' => $this->getConfiguration()->getUsers()
            ]);
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
        // Check if there is no maintenance
        $this->getGoodDataApi(false);

        // Init parameters
        $this->checkParams(['writerId', 'email', 'pid']);
        $this->checkWriterExistence();

        if (!$this->getSharedStorage()->projectBelongsToWriter($this->projectId, $this->writerId, $this->params['pid'])) {
            throw new UserException($this->translator->trans('parameters.sso_wrong_pid'));
        }

        if (!empty($this->params['createUser']) && $this->params['createUser'] == 1 && !$this->getConfiguration()->getUser($this->params['email'])) {
            $result = $this->postUsersAction();
            $jsonResult = json_decode($result->getContent(), true);
            $jobFinished = false;
            $i = 1;
            /** @var Search $jobSearch */
            $jobSearch = $this->container->get('gooddata_writer.elasticsearch.search');
            do {
                $jobData = $jobSearch->getJob($jsonResult['job']);
                if (!$jobData) {
                    throw new UserException(sprintf("Job '%d' not found", $this->params['jobId']));
                }
                if (isset($jobData['status']) && Job::isJobFinished($jobData['status'])) {
                    $jobFinished = true;
                }
                if (!$jobFinished) {
                    sleep($i * 5);
                }
                $i++;
            } while (!$jobFinished);

            if ($jobData['status'] == Job::STATUS_SUCCESS) {
                if (!empty($jobData['result']['alreadyExists'])) {
                    throw new UserException($this->translator->trans('result.cancelled'));
                }
                // Do nothing
            } elseif ($jobData['status'] == Job::STATUS_CANCELLED) {
                throw new UserException($this->translator->trans('result.cancelled'));
            } else {
                $e = new UserException(!empty($jobData['result']['error'])? $jobData['result']['error'] : $this->translator->trans('result.unknown'));
                $e->setData(['result' => $jobData['result'], 'logs' => $jobData['logs']]);
                throw $e;
            }


            $result = $this->postProjectUsersAction();
            $jsonResult = json_decode($result->getContent(), true);
            $jobFinished = false;
            $i = 1;
            do {
                $jobData = $jobSearch->getJob($jsonResult['job']);
                if (!$jobData) {
                    throw new UserException(sprintf("Job '%d' not found", $this->params['jobId']));
                }
                if (isset($jobData['status']) && Job::isJobFinished($jobData['status'])) {
                    $jobFinished = true;
                }
                if (!$jobFinished) {
                    sleep($i * 5);
                }
                $i++;
            } while (!$jobFinished);

            if ($jobData['status'] == Job::STATUS_SUCCESS) {
                // Do nothing
            } elseif ($jobData['status'] == Job::STATUS_CANCELLED) {
                throw new UserException($this->translator->trans('result.cancelled'));
            } else {
                $e = new UserException(!empty($jobData['result']['error'])? $jobData['result']['error'] : $this->translator->trans('result.unknown'));
                $e->setData(['result' => $jobData['result'], 'logs' => $jobData['logs']]);
                throw $e;
            }
        }

        if (!$this->getSharedStorage()->userBelongsToWriter($this->projectId, $this->writerId, $this->params['email'])) {
            throw new UserException($this->translator->trans('parameters.sso_wrong_email'));
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
        /** @var Temp $temp */
        $temp = $this->container->get('syrup.temp');
        $ssoLink = SSO::url($domainUser->username, $ssoProvider, $passphrase, $temp, $targetUrl, $this->params['email'], $validity);

        if (null == $ssoLink) {
            $e = new SyrupComponentException(500, $this->translator->trans('error.sso_unknown'));
            $e->setData(['params' => $this->params]);
            throw $e;
        }

        return $this->createApiResponse([
            'ssoLink' => $ssoLink
        ]);
    }



    /**
     * Call GD Api with POST request
     *
     * @Route("/proxy")
     * @Method({"POST"})
     */
    public function postProxyAction()
    {
        $taskName = 'proxyCall';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * Call GD Api with GET request
     *
     * @Route("/proxy")
     * @Method({"GET"})
     */
    public function getProxyAction()
    {
        $this->checkParams(['writerId', 'query']);
        $this->checkWriterExistence();

        // query validation
        try {
            // clean url - remove domain
            $query = Url::factory(urldecode($this->params['query']));

            $url = Url::buildUrl([
                'path' => $query->getPath(),
                'query' => $query->getQuery(),
            ]);
        } catch (InvalidArgumentException $e) {
            throw new UserException($this->translator->trans('parameters.query'));
        }

        $restApi = $this->getGoodDataApi();
        $response = $restApi->get($url);

        return $this->createApiResponse([
            'response' => $response
        ]);
    }



    /**
     * Create new user filter
     *
     * @Route("/filters")
     * @Method({"POST"})
     */
    public function postFiltersAction()
    {
        $taskName = 'createFilter';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * Delete user filter
     *
     * @Route("/filters")
     * @Method({"DELETE"})
     */
    public function deleteFiltersAction()
    {
        $taskName = 'deleteFilter';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
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
        $this->checkParams(['writerId']);
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

        return $this->createApiResponse([
            'filters' => $filters
        ]);
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
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        if (isset($this->params['pid'])) {
            $filters = $this->getConfiguration()->getFiltersProjectsByPid($this->params['pid']);
        } elseif (isset($this->params['filter'])) {
            $filters = $this->getConfiguration()->getFiltersProjectsByFilter($this->params['filter']);
        } else {
            $filters = $this->getConfiguration()->getFiltersProjects();
        }

        return $this->createApiResponse([
            'filters' => $filters
        ]);
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
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        if (isset($this->params['email'])) {
            $filters = $this->getConfiguration()->getFiltersUsersByEmail($this->params['email']);
        } elseif (isset($this->params['filter'])) {
            $filters = $this->getConfiguration()->getFiltersUsersByFilter($this->params['filter']);
        } else {
            $filters = $this->getConfiguration()->getFiltersUsers();
        }

        return $this->createApiResponse([
            'filters' => $filters
        ]);
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
        $taskName = 'assignFiltersToUser';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * Synchronize filters from writer's configuration to GoodData project
     *
     * @Route("/sync-filters")
     * @Method({"POST"})
     */
    public function postSyncFiltersAction()
    {
        $taskName = 'syncFilters';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();

        $projects = empty($params['pid']) ? $this->getProjectsToUse() : [$params['pid']];
        foreach ($projects as $pid) {
            $job->addTask($taskName, ['pid' => $pid]);
        }

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }



    /**
     * Generates LDM model of writer
     * @Route("/ldm")
     * @Method({"GET"})
     */
    public function getLdmAction()
    {
        $this->checkParams(['writerId']);
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
        $taskName = 'UploadDateDimension';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();

        foreach ($this->getProjectsToUse() as $pid) {
            $job->addTask($taskName, ['pid' => $pid, 'name' => $params['name'], 'includeTime' => $params['includeTime']]);
        }

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * @Route("/update-model")
     * @Method({"POST"})
     * params: pid, queue
     */
    public function postUpdateModel()
    {
        $taskName = 'updateModel';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();

        $definition = $this->getConfiguration()->getDataSetDefinition($params['tableId']);
        foreach ($this->getProjectsToUse() as $pid) {
            $job->addTask($taskName, ['pid' => $pid, 'tableId' => $params['tableId']], $definition);
        }
        $job->uploadDefinitions($this->s3Client);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * @Route("/load-data")
     * @Method({"POST"})
     * params: pid, queue
     */
    public function postLoadData()
    {
        $taskName = 'loadData';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();

        foreach ($params['tables'] as $tableId) {
            $definition = $this->getConfiguration()->getDataSetDefinition($tableId);
            foreach ($this->getProjectsToUse() as $pid) {
                $loadParams = ['pid' => $pid, 'tableId' => $tableId];
                if (isset($params['incrementalLoad'])) {
                    $loadParams['incrementalLoad'] = $params['incrementalLoad'];
                }
                $job->addTask($taskName, $loadParams, $definition);
            }
        }
        $job->uploadDefinitions($this->s3Client);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * @Route("/load-data-multi")
     * @Method({"POST"})
     * params: pid, queue
     */
    public function postLoadDataMulti()
    {
        $taskName = 'loadDataMulti';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();

        foreach ($this->getProjectsToUse() as $pid) {
            $definition = [];
            foreach ($params['tables'] as $tableId) {
                $definition[$tableId] = [
                    'columns' => $this->getConfiguration()->getDataSetDefinition($tableId),
                    'dataset' => $this->getConfiguration()->getDataSet($tableId)
                ];
            }

            $loadParams = ['pid' => $pid, 'tables' => $params['tables']];
            if (isset($params['incrementalLoad'])) {
                $loadParams['incrementalLoad'] = $params['incrementalLoad'];
            }
            $job->addTask($taskName, $loadParams, $definition);
        }
        $job->uploadDefinitions($this->s3Client);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
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
        $this->checkParams(['writerId', 'tableId']);
        $this->checkWriterExistence();

        $restApi = $this->getGoodDataApi();

        $definition = $this->getConfiguration()->getDataSetDefinition($this->params['tableId']);
        $projectsToUse = $this->getProjectsToUse();

        $job = $this->getJob();

        // Create date dimensions
        $dateDimensionsToLoad = [];
        $dateDimensions = [];
        if ($definition['columns']) {
            foreach ($definition['columns'] as $columnName => $column) {
                if ($column['type'] == 'DATE') {
                    if (!$dateDimensions) {
                        $dateDimensions = $this->getConfiguration()->getDateDimensions();
                    }

                    $dimension = $column['schemaReference'];
                    if (!isset($dateDimensions[$dimension])) {
                        throw new UserException($this->translator->trans('configuration.dimension_not_found %d %c', ['%d' => $dimension, '%c' => $columnName]));
                    }

                    if (!$dateDimensions[$dimension]['isExported'] && !in_array($dimension, $dateDimensionsToLoad)) {
                        $dateDimensionsToLoad[] = $dimension;

                        foreach ($projectsToUse as $pid) {
                            $params = [
                                'pid' => $pid,
                                'name' => $dimension,
                                'includeTime' => $dateDimensions[$dimension]['includeTime']
                            ];
                            $job->addTask('uploadDateDimension', $params);
                        }
                    }
                }
            }
        }

        $jobData = null;
        $tableConfiguration = $this->getConfiguration()->getDataSet($this->params['tableId']);
        foreach ($projectsToUse as $pid) {
            // Make decision about creating / updating of data set
            $existingDataSets = $restApi->getDataSets($pid);
            $dataSetExists = in_array($tableConfiguration['identifier'], array_keys($existingDataSets));
            $lastGoodDataUpdate = empty($existingDataSets[$tableConfiguration['identifier']]['lastChangeDate'])? null
                : Model::getTimestampFromApiDate($existingDataSets[$tableConfiguration['identifier']]['lastChangeDate']);
            $lastConfigurationUpdate = empty($tableConfiguration['lastChangeDate'])? null : strtotime($tableConfiguration['lastChangeDate']);
            $doUpdate = $dataSetExists && $lastConfigurationUpdate && (!$lastGoodDataUpdate || $lastGoodDataUpdate < $lastConfigurationUpdate);

            if (!$dataSetExists || $doUpdate) {
                $params = [
                    'pid' => $pid,
                    'tableId' => $this->params['tableId']
                ];
                $job->addTask('updateModel', $params, $definition);
            }

            $params = [
                'pid' => $pid,
                'tableId' => $this->params['tableId']
            ];
            if (isset($this->params['incrementalLoad'])) {
                $params['incrementalLoad'] = $this->params['incrementalLoad'];
            }
            $job->addTask('loadData', $params, $definition);
        }
        $job->uploadDefinitions($this->s3Client);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * Upload project to GoodData
     *
     * @Route("/upload-project")
     * @Method({"POST"})
     */
    public function postUploadProjectAction()
    {
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        $restApi = $this->getGoodDataApi();
        $projectsToUse = $this->getProjectsToUse();

        $this->getConfiguration()->getDateDimensions();

        $sortedDataSets = $this->getConfiguration()->getSortedDataSets();

        $job = $this->getJob();


        // Create date dimensions
        $dateDimensionsToLoad = [];
        $dateDimensions = [];
        foreach ($sortedDataSets as $dataSet) {
            if ($dataSet['definition']['columns']) {
                foreach ($dataSet['definition']['columns'] as $columnName => $column) {
                    if ($column['type'] == 'DATE') {
                        if (!$dateDimensions) {
                            $dateDimensions = $this->getConfiguration()->getDateDimensions();
                        }

                        $dimension = $column['schemaReference'];
                        if (!isset($dateDimensions[$dimension])) {
                            throw new UserException($this->translator->trans(
                                'configuration.dimension_not_found %d %c %t',
                                ['%d' => $dimension, '%c' => $columnName, '%t' => $dataSet['tableId']]
                            ));
                        }

                        if (!$dateDimensions[$dimension]['isExported'] && !in_array($dimension, $dateDimensionsToLoad)) {
                            $dateDimensionsToLoad[] = $dimension;

                            foreach ($projectsToUse as $pid) {
                                $params = [
                                    'pid' => $pid,
                                    'name' => $dimension,
                                    'includeTime' => $dateDimensions[$dimension]['includeTime']
                                ];
                                $job->addTask('uploadDateDimension', $params);
                            }
                        }
                    }
                }
            }
        }


        $existingDataSets = [];
        foreach ($sortedDataSets as $dataSet) {
            foreach ($projectsToUse as $pid) {
                // Make decision about creating / updating of data set
                if (!isset($existingDataSets[$pid])) {
                    $existingDataSets[$pid] = $restApi->getDataSets($pid);
                }
                $dataSetExists = in_array($dataSet['identifier'], array_keys($existingDataSets[$pid]));
                $lastGoodDataUpdate = empty($existingDataSets[$pid][$dataSet['identifier']]['lastChangeDate'])? null
                    : Model::getTimestampFromApiDate($existingDataSets[$pid][$dataSet['identifier']]['lastChangeDate']);
                $lastConfigurationUpdate = empty($dataSet['lastChangeDate'])? null : strtotime($dataSet['lastChangeDate']);
                $doUpdate = $dataSetExists && $lastConfigurationUpdate && (!$lastGoodDataUpdate || $lastGoodDataUpdate < $lastConfigurationUpdate);

                if (!$dataSetExists || $doUpdate) {
                    $params = [
                        'pid' => $pid,
                        'tableId' => $dataSet['tableId']
                    ];
                    $job->addTask('updateModel', $params, $dataSet['definition']);
                }

                $params = [
                    'pid' => $pid,
                    'tableId' => $dataSet['tableId']
                ];
                if (isset($this->params['incrementalLoad'])) {
                    $params['incrementalLoad'] = $this->params['incrementalLoad'];
                }
                $job->addTask('loadData', $params, $dataSet['definition']);
            }
        }

        // Execute reports
        foreach ($projectsToUse as $pid) {
            $job->addTask('executeReports', ['pid' => $pid]);
        }
        $job->uploadDefinitions($this->s3Client);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * Reset dataSet and remove it from GoodData project
     *
     * @Route("/reset-table")
     * @Method({"POST"})
     */
    public function postResetTableAction()
    {
        $taskName = 'resetTable';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }


    /**
     * Reset GoodData project
     *
     * @Route("/reset-project")
     * @Method({"POST"})
     */
    public function postResetProjectAction()
    {
        $taskName = 'resetProject';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }

    /**
     * Execute Reports in GoodData
     *
     * @Route("/execute-reports")
     * @Method({"POST"})
     */
    public function postExecuteReportsAction()
    {
        $taskName = 'executeReports';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }


    /**
     * Export report data from GoodData
     *
     * @Route("/export-report")
     * @Method({"POST"})
     */
    public function postExportReportAction()
    {
        $taskName = 'exportReport';
        $task = $this->getTaskClass($taskName);
        $params = $task->prepare($this->params);

        $job = $this->getJob();
        $job->addTask($taskName, $params);

        $jobId = $this->getJobFactory()->save($job);
        $this->getJobFactory()->enqueue($jobId);
        return $this->createPollResponse($jobId);
    }



    /**
     * Get visual model
     *
     * @Route("/model")
     * @Method({"GET"})
     */
    public function getModelAction()
    {
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        $model = new Graph();
        $dimensionsUrl = sprintf(
            '%s/admin/projects-new/%s/gooddata?config=%s#/date-dimensions',
            $this->container->getParameter('storage_api.url'),
            $this->projectId,
            $this->writerId
        );
        $tableUrl = sprintf(
            '%s/admin/projects-new/%s/gooddata?config=%s#/table/',
            $this->container->getParameter('storage_api.url'),
            $this->projectId,
            $this->writerId
        );
        $model->setTableUrl($tableUrl);
        $model->setDimensionsUrl($dimensionsUrl);

        try {
            $result = $model->getGraph($this->getConfiguration());
        } catch (GraphTtlException $e) {
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
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        if (isset($this->params['tableId'])) {
            // Table detail
            return $this->createApiResponse([
                'table' => $this->getConfiguration()->getDataSetForApi($this->params['tableId'])
            ]);
        } elseif (isset($this->params['connection'])) {
            return $this->createApiResponse([
                'tables' => $this->getConfiguration()->getDataSetsWithConnectionPoint()
            ]);
        } else {
            return $this->createApiResponse([
                'tables' => $this->getConfiguration()->getDataSets()
            ]);
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
        $this->checkParams(['writerId', 'tableId']);
        $this->checkWriterExistence();
        if (!in_array($this->params['tableId'], $this->getConfiguration()->getOutputSapiTables())) {
            throw new UserException($this->translator->trans('parameters.tableId'));
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
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        foreach ($this->getConfiguration()->getDataSets() as $dataSet) {
            if (!empty($dataSet['isExported'])) {
                $this->getConfiguration()->updateDataSetDefinition($dataSet['id'], 'isExported', 0);
            }
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
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();

        return $this->createApiResponse([
            'dimensions' => (object) $this->getConfiguration()->getDateDimensions(isset($this->params['usage']))
        ]);
    }


    /**
     * Delete configured date dimension
     *
     * @Route("/date-dimensions")
     * @Method({"DELETE"})
     */
    public function deleteDateDimensionsAction()
    {
        $this->checkParams(['writerId', 'name']);
        $this->checkWriterExistence();

        $dimensions = $this->getConfiguration()->getDateDimensions();
        if (isset($dimensions[$this->params['name']])) {
            if (!empty($dimensions[$this->params['name']]['isExported'])) {
                throw new UserException($this->translator->trans('error.dimension_uploaded'));
            }
            $this->getConfiguration()->deleteDateDimension($this->params['name']);
            return $this->createApiResponse();
        } else {
            throw new UserException($this->translator->trans('parameters.dimension_name'));
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
        $this->checkParams(['writerId', 'name']);
        $this->checkWriterExistence();

        $this->params['name'] = trim($this->params['name']);
        $template = !empty($this->params['template'])? $this->params['template'] : null;

        $dimensions = $this->getConfiguration()->getDateDimensions();
        if (!isset($dimensions[$this->params['name']])) {
            $this->getConfiguration()->saveDateDimension($this->params['name'], !empty($this->params['includeTime']), $template);
        }

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

    private function getTaskClass($taskName)
    {
        /** @var Factory $taskFactory */
        $taskFactory = $this->container->get('gooddata_writer.task_factory');
        $taskFactory
            ->setStorageApiClient($this->storageApi)
            ->setConfiguration($this->getConfiguration())
            ->setEventLogger($this->eventLogger);
        return $taskFactory->create($taskName);
    }

    private function getConfiguration()
    {
        if (!$this->writerId) {
            throw new UserException($this->translator->trans('parameters.writerId.required'));
        }
        if (!$this->configuration) {
            $this->configuration = new Configuration(new CachedClient($this->storageApi), $this->getSharedStorage());
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

    private function getJobFactory()
    {
        if (!$this->jobFactory) {
            $this->jobFactory = $this->container->get('gooddata_writer.job_factory');
            $this->jobFactory->setConfiguration($this->getConfiguration());
            $this->jobFactory->setStorageApiClient($this->storageApi);
        }
        return $this->jobFactory;
    }

    private function getGoodDataApi($login = true)
    {
        /** @var RestApi $restApi */
        $restApi = $this->container->get('gooddata_writer.rest_api');

        $bucketAttributes = $this->getConfiguration()->getBucketAttributes();
        if (!empty($bucketAttributes['gd']['backendUrl'])) {
            $restApi->setBaseUrl($bucketAttributes['gd']['backendUrl']);
        }
        if (!$restApi->ping()) {
            return $this->createMaintenanceResponse();
        }
        if ($login) {
            $restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        }
        return $restApi;
    }


    private function createApiResponse($response = [], $statusCode = 200, $statusMessage = null)
    {
        if (!$statusMessage) {
            $statusMessage = ($statusCode >= 300)? 'error' : 'ok';
        }
        $responseBody = [
            'status' => $statusMessage
        ];

        if ($this->stopWatch->isStarted(self::STOPWATCH_NAME_REQUEST)) {
            $event = $this->stopWatch->stop(self::STOPWATCH_NAME_REQUEST);
            $responseBody['duration']  = $event->getDuration();
        }

        if (null != $response) {
            $responseBody = array_merge($response, $responseBody);
        }

        return $this->createJsonResponse($responseBody, $statusCode);
    }

    public function createMaintenanceResponse()
    {
        return $this->createApiResponse([
            'error' => 'There is undergoing maintenance on GoodData backend, please try again later.'
        ], 503, 'maintenance');
    }

    private function createPollResponse($jobId)
    {
        return $this->createJsonResponse([
            'id' => $jobId,
            'url' => $this->getJobUrl($jobId)
        ], 202);
    }

    private function checkParams($required)
    {
        foreach ($required as $k) {
            if (empty($this->params[$k])) {
                throw new UserException($this->translator->trans('parameters.required %1', ['%1' => $k]));
            }
        }
    }

    private function checkWriterExistence()
    {
        if (!$this->getSharedStorage()->writerExists($this->projectId, $this->writerId)) {
            throw new UserException($this->translator->trans('parameters.writerId.not_found'));
        }
    }


    protected function getProjectsToUse()
    {
        $this->configuration->checkTable(Configuration::PROJECTS_TABLE_NAME);
        $projects = [];
        foreach ($this->getConfiguration()->getProjects() as $project) {
            if ($project['active']) {
                if (in_array($project['pid'], $projects)) {
                    throw new UserException($this->translator->trans('configuration.project.duplicated %1', ['%1' => $project['pid']]));
                }
                if (!isset($this->params['pid']) || $project['pid'] == $this->params['pid']) {
                    $projects[] = $project['pid'];
                }
            }
        }
        if (isset($params['pid']) && !count($projects)) {
            throw new UserException($this->translator->trans('parameters.pid_not_configured'));
        }
        return $projects;
    }

    /**
     * @deprecated
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
        $this->checkParams(['writerId']);
        $this->checkWriterExistence();
        if (empty($this->params['jobId'])) {
            $days = isset($this->params['days']) ? $this->params['days'] : 3;
            $tableId = empty($this->params['tableId']) ? null : $this->params['tableId'];
            $command = empty($this->params['command']) ? null : $this->params['command'];
            $tokenId = empty($this->params['tokenId']) ? null : $this->params['tokenId'];
            $status = empty($this->params['status']) ? null : $this->params['status'];
            $jobs = $this->getSharedStorage()->fetchJobs($this->projectId, $this->writerId, $days);
            $result = [];
            foreach ($jobs as $job) {
                if ((empty($command) || $command == $job['command']) && (empty($tokenId) || $tokenId == $job['tokenId'])
                    && (empty($status) || $status == $job['status'])) {
                    if (empty($tableId) || (!empty($job['parameters']['tableId']) && $job['parameters']['tableId'] == $tableId)) {
                        $result[] = self::jobToApiResponse($job, $this->s3Client);
                    }
                }
            }
            return $this->createApiResponse([
                'jobs' => $result
            ]);
        } else {
            if (is_array($this->params['jobId'])) {
                throw new UserException($this->translator->trans('parameters.jobId_number'));
            }
            $job = $this->getSharedStorage()->fetchJob($this->params['jobId'], $this->projectId, $this->writerId);
            if (!$job) {
                // Fallback for ES jobs
                /** @var Search $jobSearch */
                $jobSearch = $this->container->get('gooddata_writer.elasticsearch.search');
                $job = $jobSearch->getJob($this->params['jobId']);
                if (!$job) {
                    throw new UserException($this->translator->trans('parameters.job'));
                }
                $params = $job->getParams();
                $job = [
                    'id' => (int) $job->getId(),
                    'batchId' => (int) $job->getId(),
                    'runId' => (int) $job->getRunId(),
                    'projectId' => (int) $job->getProject()['id'],
                    'writerId' => isset($params['writerId']) ? $params['writerId'] : null,
                    'queueId' => $job->getLockName(),
                    'token' => [
                        'id' => (int) $job->getToken()['id'],
                        'description' => $job->getToken()['description'],
                    ],
                    'createdTime' => $job->getCreatedTime(),
                    'startTime' => $job->getStartTime(),
                    'endTime' => $job->getEndTime(),
                    'command' => $job->getCommand(),
                    'dataset' => null,
                    'parameters' => $params,
                    'result' => $job->getResult(),
                    'gdWriteStartTime' => false,
                    'status' => $job->getStatus(),
                    'logs' => []
                ];
            } else {
                $job = self::jobToApiResponse($job, $this->s3Client);
            }
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
        $this->checkParams(['writerId', 'batchId']);
        $this->checkWriterExistence();
        $jobs = $this->getSharedStorage()->fetchBatch($this->params['batchId'], $this->projectId, $this->writerId);
        if (!count($jobs)) {
            // Fallback for ES jobs
            /** @var Search $jobSearch */
            $jobSearch = $this->container->get('gooddata_writer.elasticsearch.search');
            $job = $jobSearch->getJob($this->params['batchId']);
            if (!$job) {
                throw new UserException(sprintf("Batch '%d' not found", $this->params['batchId']));
            }
            $params = $job->getParams();
            $batch = [
                'batchId' => (int) $job->getId(),
                'runId' => (int) $job->getRunId(),
                'projectId' => (int) $job->getProject()['id'],
                'writerId' => isset($params['writerId']) ? $params['writerId'] : null,
                'queueId' => $job->getLockName(),
                'token' => [
                    'id' => (int) $job->getToken()['id'],
                    'description' => $job->getToken()['description'],
                ],
                'createdTime' => $job->getCreatedTime(),
                'startTime' => $job->getStartTime(),
                'endTime' => $job->getEndTime(),
                'command' => $job->getCommand(),
                'dataset' => null,
                'jobs' => isset($params['tasks']) ? $params['tasks'] : [],
                'result' => $job->getResult(),
                'gdWriteStartTime' => false,
                'status' => $job->getStatus(),
                'logs' => []
            ];
        } else {
            $batch = self::batchToApiResponse($this->params['batchId'], $jobs);
        }
        return $this->createJsonResponse($batch);
    }

    public static function jobToApiResponse(array $job, S3Client $s3Client = null)
    {
        if (isset($job['parameters']['accessToken'])) {
            $job['parameters']['accessToken'] = '***';
        }
        if (isset($job['parameters']['password'])) {
            $job['parameters']['password'] = '***';
        }
        $logs = is_array($job['logs']) ? $job['logs'] : [];
        if (!empty($job['definition'])) {
            $logs['DataSet Definition'] = $job['definition'];
        }
        // Find private links and make them accessible
        if ($s3Client) {
            foreach ($logs as &$log) {
                if (is_array($log)) {
                    foreach ($log as &$v) {
                        $url = parse_url($v);
                        if (empty($url['host'])) {
                            $v = $s3Client->getPublicLink($v);
                        }
                    }
                } else {
                    $url = parse_url($log);
                    if (empty($url['host'])) {
                        $log = $s3Client->getPublicLink($log);
                    }
                }
            }
        }
        $result = [
            'id' => (int) $job['id'],
            'batchId' => (int) $job['batchId'],
            'runId' => (int) $job['runId'],
            'projectId' => (int) $job['projectId'],
            'writerId' => (string) $job['writerId'],
            'queueId' => !empty($job['queueId']) ? $job['queueId'] : sprintf('%s.%s.%s', $job['projectId'], $job['writerId'], Job::PRIMARY_QUEUE),
            'token' => [
                'id' => (int) $job['tokenId'],
                'description' => $job['tokenDesc'],
            ],
            'createdTime' => date('c', strtotime($job['createdTime'])),
            'startTime' => !empty($job['startTime']) ? date('c', strtotime($job['startTime'])) : null,
            'endTime' => !empty($job['endTime']) ? date('c', strtotime($job['endTime'])) : null,
            'command' => $job['command'],
            'dataset' => $job['dataset'],
            'parameters' => $job['parameters'],
            'result' => $job['result'],
            'gdWriteStartTime' => false,
            'status' => $job['status'],
            'logs' => $logs
        ];
        return $result;
    }

    public static function batchToApiResponse($batchId, array $jobs, S3Client $s3Client = null)
    {
        $data = [
            'batchId' => (int)$batchId,
            'projectId' => null,
            'writerId' => null,
            'queueId' => null,
            'createdTime' => date('c'),
            'startTime' => date('c'),
            'endTime' => null,
            'status' => null,
            'jobs' => []
        ];
        $cancelledJobs = 0;
        $waitingJobs = 0;
        $processingJobs = 0;
        $errorJobs = 0;
        $successJobs = 0;
        foreach ($jobs as $job) {
            $job = self::jobToApiResponse($job, $s3Client);
            if (!$data['projectId']) {
                $data['projectId'] = $job['projectId'];
            } elseif ($data['projectId'] != $job['projectId']) {
                throw new ApplicationException(sprintf(
                    'ProjectId of job %s: %s does not match projectId %s of previous job.',
                    $job['id'],
                    $job['projectId'],
                    $data['projectId']
                ));
            }
            if (!$data['writerId']) {
                $data['writerId'] = $job['writerId'];
            } elseif ($data['writerId'] != $job['writerId']) {
                throw new ApplicationException(sprintf(
                    'WriterId of job %s: %s does not match writerId %s of previous job.',
                    $job['id'],
                    $job['projectId'],
                    $data['projectId']
                ));
            }
            if ($job['queueId'] && $job['queueId'] != Job::PRIMARY_QUEUE) {
                $data['queueId'] = $job['queueId'];
            }
            if ($job['createdTime'] < $data['createdTime']) {
                $data['createdTime'] = $job['createdTime'];
            }
            if ($job['startTime'] < $data['startTime']) {
                $data['startTime'] = $job['startTime'];
            }
            if ($job['endTime'] > $data['endTime']) {
                $data['endTime'] = $job['endTime'];
            }
            $data['jobs'][] = $job;
            if ($job['status'] == Job::STATUS_WAITING) {
                $waitingJobs++;
            } elseif ($job['status'] == Job::STATUS_PROCESSING) {
                $processingJobs++;
            } elseif ($job['status'] == Job::STATUS_CANCELLED) {
                $cancelledJobs++;
            } elseif ($job['status'] == Job::STATUS_ERROR) {
                $errorJobs++;
                $data['result'][$job['id']] = $job['result'];
            } else {
                $successJobs++;
            }
        }
        if (!$data['queueId']) {
            $data['queueId'] = sprintf('%s.%s.%s', $data['projectId'], $data['writerId'], Job::PRIMARY_QUEUE);
        }
        if ($cancelledJobs > 0) {
            $data['status'] = Job::STATUS_CANCELLED;
        } elseif ($processingJobs > 0) {
            $data['status'] = Job::STATUS_PROCESSING;
        } elseif ($waitingJobs > 0) {
            $data['status'] = Job::STATUS_WAITING;
        } elseif ($errorJobs > 0) {
            $data['status'] = Job::STATUS_ERROR;
        } else {
            $data['status'] = Job::STATUS_SUCCESS;
        }
        if ($data['status'] == Job::STATUS_WAITING && $data['startTime']) {
            $data['startTime'] = null;
        }
        return $data;
    }
}
