<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\StorageApi\EventLogger;
use Keboola\GoodDataWriter\Aws\S3Client;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\Job\Metadata\JobFactory;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\Event;
use Keboola\Syrup\Exception\UserException;
use Monolog\Logger;
use Symfony\Component\Translation\TranslatorInterface;
use Keboola\Temp\Temp;

abstract class AbstractTask
{
    /**
     * @var JobFactory
     */
    protected $jobFactory;
    /**
     * @var Factory
     */
    protected $taskFactory;
    /**
     * @var Configuration
     */
    protected $configuration;
    /**
     * @var SharedStorage
     */
    protected $sharedStorage;
   /**
     * @var S3Client
     */
    protected $s3Client;
    /**
     * @var Temp
     */
    protected $temp;
    /**
     * @var TranslatorInterface
     */
    protected $translator;
    /**
     * For CsvHandler
     * @var Logger
     */
    protected $logger;
    /**
     * @var StorageApiClient $storageApiClient
     */
    protected $storageApiClient;
    /**
     * @var \Keboola\GoodDataWriter\GoodData\User
     */
    private $domainUser;
    /**
     * @var \Keboola\GoodDataWriter\StorageApi\EventLogger
     */
    protected $eventLogger;

    private $tmpDir;

    protected $scriptsPath;
    protected $gdDomain;
    protected $gdSsoProvider;
    protected $gdProjectNamePrefix;
    protected $gdUsernameDomain;


    public function __construct(
        $gdConfig,
        Configuration $configuration,
        SharedStorage $sharedStorage,
        StorageApiClient $storageApiClient,
        RestApi $restApi
    ) {
        $this->configuration = $configuration;
        $this->sharedStorage = $sharedStorage;
        $this->storageApiClient = $storageApiClient;
        $this->restApi = $restApi;

        if (!isset($gdConfig['access_token'])) {
            throw new \Exception("Key 'access_token' is missing from gd config");
        }
        $this->gdAccessToken = $gdConfig['access_token'];
        if (!isset($gdConfig['domain'])) {
            throw new \Exception("Key 'domain' is missing from gd config");
        }
        $this->gdDomain = $this->configuration->gdDomain? $this->configuration->gdDomain : $gdConfig['domain'];
        $this->gdSsoProvider = isset($gdConfig['ssoProvider'])? $gdConfig['ssoProvider'] : Model::SSO_PROVIDER;
        $this->gdProjectNamePrefix = isset($gdConfig['projectNamePrefix'])? $gdConfig['projectNamePrefix'] : Model::PROJECT_NAME_PREFIX;
        $this->gdUsernameDomain = isset($gdConfig['usernameDomain'])? $gdConfig['usernameDomain'] : Model::USERNAME_DOMAIN;
    }

    protected function initRestApi(Job $job)
    {
        $this->restApi->setJobId($job->getId());
        $this->restApi->setRunId($job->getRunId());

        try {
            $bucketAttributes = $this->configuration->getBucketAttributes();
            if (!empty($bucketAttributes['gd']['backendUrl'])) {
                $this->restApi->setBaseUrl($bucketAttributes['gd']['backendUrl']);
            }
        } catch (UserException $e) {
            // Ignore
        }
    }


    abstract public function prepare($params);
    abstract public function run(Job $job, $taskId, array $params = [], $definitionFile = null);


    protected function getDefinition($definitionFile)
    {
        $definition = $this->s3Client->downloadFile($definitionFile);
        $definition = json_decode($definition, true);
        if (!$definition) {
            throw new \Exception('Download from S3 failed: ' . $definitionFile);
        }
        return $definition;
    }

    protected function getTmpDir($jobId)
    {
        $this->tmpDir = sprintf('%s/%s', $this->temp->getTmpFolder(), $jobId);
        if (!file_exists($this->temp->getTmpFolder())) {
            mkdir($this->temp->getTmpFolder());
        }
        if (!file_exists($this->tmpDir)) {
            mkdir($this->tmpDir);
        }

        return $this->tmpDir;
    }

    protected function getDomainUser()
    {
        if (!$this->domainUser) {
            $this->domainUser = $this->sharedStorage->getDomainUser($this->gdDomain);
        }
        return $this->domainUser;
    }

    public function setJobFactory(JobFactory $factory)
    {
        $this->jobFactory = $factory;
        return $this;
    }

    public function setTaskFactory(Factory $factory)
    {
        $this->taskFactory = $factory;
        return $this;
    }

    public function setTemp(Temp $temp)
    {
        $this->temp = $temp;
        return $this;
    }

    public function setEventLogger(EventLogger $logger)
    {
        $this->eventLogger = $logger;
        return $this;
    }

    public function setLogger(Logger $logger)
    {
        $this->logger = $logger;
        return $this;
    }

    public function setTranslator(TranslatorInterface $translator)
    {
        $this->translator = $translator;
        return $this;
    }

    public function setS3Client(S3Client $s3Client)
    {
        $this->s3Client = $s3Client;
        return $this;
    }

    public function setScriptsPath($scriptsPath)
    {
        $this->scriptsPath = $scriptsPath;
        return $this;
    }

    public function logEvent($message, $taskId, $jobId, $runId, $params = [], $duration = null, $type = Event::TYPE_INFO)
    {
        $className = current(array_slice(explode('\\', get_class($this)), -1)); //@TODO self::class in PHP 5.5
        $message = sprintf('Task %d (%s): %s', $taskId, $className, $message);
        $this->eventLogger->log($jobId, $runId, $message, $params, $duration, $type);
    }

    protected function checkParams($params, $required)
    {
        foreach ($required as $k) {
            if (empty($params[$k])) {
                throw new UserException($this->translator->trans('parameters.required %1', ['%1' => $k]));
            }
        }
    }

    protected function checkWriterExistence($writerId)
    {
        $tokenInfo = $this->storageApiClient->getLogData();
        $projectId = $tokenInfo['owner']['id'];

        if (!$this->sharedStorage->writerExists($projectId, $writerId)) {
            throw new UserException($this->translator->trans('parameters.writerId.not_found'));
        }
    }
}