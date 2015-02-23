<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\StorageApi\Client as StorageApiClient;
use Monolog\Logger;
use Symfony\Component\Translation\TranslatorInterface;
use Keboola\Temp\Temp;

abstract class AbstractJob
{
    /**
     * @var JobFactory
     */
    protected $factory;
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
     * @var \Keboola\GoodDataWriter\Service\EventLogger
     */
    protected $eventLogger;

    protected $logs;

    private $tmpDir;

    protected $scriptsPath;
    protected $gdDomain;
    protected $gdSsoProvider;
    protected $gdProjectNamePrefix;
    protected $gdUsernameDomain;


    public function __construct($gdConfig, Configuration $configuration)
    {
        $this->configuration = $configuration;
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

        $this->logs = [];
    }


    abstract public function prepare($params);
    abstract public function run($job, $params, RestApi $restApi);


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

    public function setSharedStorage(SharedStorage $sharedStorage)
    {
        $this->sharedStorage = $sharedStorage;
        return $this;
    }

    public function setStorageApiClient(StorageApiClient $storageApiClient)
    {
        $this->storageApiClient = $storageApiClient;
        return $this;
    }


    public function setFactory($factory)
    {
        $this->factory = $factory;
        return $this;
    }


    public function setTemp($temp)
    {
        $this->temp = $temp;
        return $this;
    }

    public function setEventLogger($logger)
    {
        $this->eventLogger = $logger;
        return $this;
    }

    public function setLogger($logger)
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

    public function getLogs()
    {
        return $this->logs;
    }

    public function logEvent($message, $jobId, $runId, $params = [], $duration = null)
    {
        $this->eventLogger->log($jobId, $runId, $message, $params, $duration);
    }

    /**
     * @param array $params
     * @param array $required
     * @throws WrongConfigurationException
     */
    protected function checkParams($params, $required)
    {
        foreach ($required as $k) {
            if (empty($params[$k])) {
                throw new WrongConfigurationException($this->translator->trans('parameters.required %1', array('%1' => $k)));
            }
        }
    }

    protected function checkWriterExistence($writerId)
    {
        $tokenInfo = $this->storageApiClient->getLogData();
        $projectId = $tokenInfo['owner']['id'];

        if (!$this->sharedStorage->writerExists($projectId, $writerId)) {
            throw new WrongConfigurationException($this->translator->trans('parameters.writerId.not_found'));
        }
    }
}
