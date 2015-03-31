<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\StorageApi\EventLogger;
use Keboola\GoodDataWriter\Aws\S3Client;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\Job\Metadata\JobFactory;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Client;
use Keboola\GoodDataWriter\Elasticsearch\Search;
use Keboola\Syrup\Exception\UserException;
use Keboola\Temp\Temp;
use Monolog\Logger;
use Symfony\Component\Translation\TranslatorInterface;

class Factory
{
    private $gdConfig;
    private $scriptsPath;
    /** @var SharedStorage */
    private $sharedStorage;
    /** @var TranslatorInterface */
    private $translator;
    /** @var Temp */
    private $temp;
    /** @var Logger */
    private $logger;
    /** @var \Keboola\GoodDataWriter\Aws\S3Client */
    private $s3Client;
    /** @var RestApi */
    private $restApi;
    /** @var \Keboola\GoodDataWriter\Job\Metadata\JobFactory */
    private $jobFactory;
    /** @var EventLogger */
    private $eventLogger;
    /** @var Configuration */
    private $configuration;
    /** @var Client */
    private $storageApiClient;
    /** @var \Keboola\GoodDataWriter\Job\Metadata\Job */
    private $job;

    public function __construct(
        $gdConfig,
        $scriptsPath,
        SharedStorage $sharedStorage,
        RestApi $restApi,
        JobFactory $jobFactory,
        S3Client $s3Client,
        Temp $temp,
        TranslatorInterface $translator,
        Logger $logger
    ) {
        $this->gdConfig = $gdConfig;
        $this->scriptsPath = $scriptsPath;
        $this->sharedStorage = $sharedStorage;
        $this->restApi = $restApi;
        $this->jobFactory = $jobFactory;
        $this->s3Client = $s3Client;
        $this->temp = $temp;
        $this->translator = $translator;
        $this->logger = $logger;
    }

    public function setEventLogger(EventLogger $eventLogger)
    {
        $this->eventLogger = $eventLogger;
        return $this;
    }

    public function setConfiguration(Configuration $configuration)
    {
        $this->jobFactory->setConfiguration($configuration);
        $this->configuration = $configuration;
        return $this;
    }

    public function setStorageApiClient(Client $client)
    {
        $this->jobFactory->setStorageApiClient($client);
        $this->storageApiClient = $client;
        return $this;
    }

    public function setJob(Job $job)
    {
        $this->job = $job;
    }

    public function create($taskName)
    {
        $className = ucfirst($taskName);
        $taskClass = 'Keboola\GoodDataWriter\Task\\' . $className;
        if (!class_exists($taskClass)) {
            throw new UserException(sprintf("Task '%1' does not exist", $className));
        }

        /** @var \Keboola\GoodDataWriter\Task\AbstractTask $task */
        $task = new $taskClass(
            $this->gdConfig,
            $this->configuration,
            $this->sharedStorage,
            $this->storageApiClient,
            $this->restApi
        );
        $task
            ->setScriptsPath($this->scriptsPath)
            ->setEventLogger($this->eventLogger)
            ->setTranslator($this->translator)
            ->setTemp($this->temp) //For csv handler
            ->setLogger($this->logger) //For csv handler
            ->setS3Client($this->s3Client)
            ->setTaskFactory($this)
            ->setJobFactory($this->jobFactory);

        return $task;
    }
}
