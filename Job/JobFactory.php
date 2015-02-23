<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\Syrup\Service\Queue\QueueService;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Client;
use Keboola\Temp\Temp;
use Monolog\Logger;
use Symfony\Component\Translation\TranslatorInterface;

class JobFactory
{
    private $gdConfig;
    private $scriptsPath;

    /**
     * @var SharedStorage
     */
    private $sharedStorage;
    /**
     * @var Configuration
     */
    private $configuration;
    /**
     * @var Client
     */
    private $storageApiClient;
    /**
     * @var EventLogger
     */
    private $eventLogger;
    /**
     * @var TranslatorInterface
     */
    private $translator;
    /**
     * @var Temp
     */
    private $temp;
    /**
     * @var Logger
     */
    private $logger;
    /**
     * @var S3Client
     */
    private $s3Client;
    /**
     * @var QueueService
     */
    private $queue;

    public function __construct($gdConfig, $scriptsPath)
    {
        $this->gdConfig = $gdConfig;
        $this->scriptsPath = $scriptsPath;
    }

    public function setSharedStorage(SharedStorage $sharedStorage)
    {
        $this->sharedStorage = $sharedStorage;
        return $this;
    }

    public function setConfiguration(Configuration $configuration)
    {
        $this->configuration = $configuration;
        return $this;
    }

    public function setStorageApiClient(Client $storageApiClient)
    {
        $this->storageApiClient = $storageApiClient;
        return $this;
    }

    public function setEventLogger(EventLogger $eventLogger)
    {
        $this->eventLogger = $eventLogger;
        return $this;
    }

    public function setTranslator(TranslatorInterface $translator)
    {
        $this->translator = $translator;
        return $this;
    }

    public function setTemp(Temp $temp)
    {
        $this->temp = $temp;
        return $this;
    }

    public function setLogger(Logger $logger)
    {
        $this->logger = $logger;
        return $this;
    }

    public function setS3Client(S3Client $s3Client)
    {
        $this->s3Client = $s3Client;
        return $this;
    }

    public function setQueue(QueueService $queue)
    {
        $this->queue = $queue;
        return $this;
    }


    public function getJobClass($jobName)
    {
        $commandName = ucfirst($jobName);
        $commandClass = 'Keboola\GoodDataWriter\Job\\' . $commandName;
        if (!class_exists($commandClass)) {
            throw new JobProcessException($this->translator->trans('job_executor.command_not_found %1', ['%1' => $commandName]));
        }

        /**
         * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
         */
        $command = new $commandClass($this->gdConfig, $this->configuration);
        $command
            ->setSharedStorage($this->sharedStorage)
            ->setStorageApiClient($this->storageApiClient)
            ->setScriptsPath($this->scriptsPath)
            ->setEventLogger($this->eventLogger)
            ->setTranslator($this->translator)
            ->setTemp($this->temp) //For csv handler
            ->setLogger($this->logger) //For csv handler
            ->setS3Client($this->s3Client)
            ->setFactory($this);

        return $command;
    }

    public function enqueueJob($batchId, $delay = 0)
    {
        $this->queue->enqueue([
            'projectId' => $this->configuration->projectId,
            'writerId' => $this->configuration->writerId,
            'batchId' => $batchId
        ], $delay);
    }

    public function createJob($jobName, $params, $batchId = null, $queue = SharedStorage::PRIMARY_QUEUE, $others = [])
    {
        $jobId = $this->storageApiClient->generateId();
        $tokenData = $this->storageApiClient->getLogData();
        $jobData = [
            'projectId' => $this->configuration->projectId,
            'writerId' => $this->configuration->writerId,
            'command' => $jobName,
            'batchId' => $batchId? $batchId : $jobId,
            'parameters' => $params,
            'runId' => $this->storageApiClient->getRunId() ?: $jobId,
            'token' => $this->storageApiClient->token,
            'tokenId' => $tokenData['id'],
            'tokenDesc' => $tokenData['description']
        ];
        if (count($others)) {
            $jobData = array_merge($jobData, $others);
        }

        $jobData = $this->sharedStorage->createJob($jobId, $this->configuration->projectId, $this->configuration->writerId, $jobData, $queue);

        array_walk($params, function(&$val, $key) {
            if ($key == 'password') {
                $val = '***';
            }
        });
        $this->eventLogger->log(
            $jobData['id'],
            $this->storageApiClient->getRunId(),
            $this->translator->trans($this->translator->trans('log.job.created')),
            [
                'projectId' => $this->configuration->projectId,
                'writerId' => $this->configuration->writerId,
                'runId' => $this->storageApiClient->getRunId() ?: $jobId,
                'command' => $jobName,
                'params' => $params
            ]
        );

        return $jobData;
    }

    public function saveDefinition($jobId, $definition)
    {
        $definitionUrl = $this->s3Client->uploadString(sprintf('%s/definition.json', $jobId), json_encode($definition));
        $this->sharedStorage->saveJob($jobId, ['definition' => $definitionUrl]);
    }

    public function getDefinition($definitionFile)
    {
        $definition = $this->s3Client->downloadFile($definitionFile);
        $definition = json_decode($definition, true);
        if (!$definition) {
            throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . $definitionFile);
        }
        return $definition;
    }

    public function createBatchId()
    {
        return $this->storageApiClient->generateId();
    }
}
