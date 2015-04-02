<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Guzzle\Http\Exception\CurlException;
use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Exception\QueueUnavailableException;
use Keboola\GoodDataWriter\Exception\CsvHandlerNetworkException;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Job\JobFactory;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\Event as StorageApiEvent;
use Keboola\StorageApi\Exception as StorageApiException;
use Monolog\Logger;
use Symfony\Component\Translation\TranslatorInterface;
use Keboola\Syrup\Exception\UserException;
use Keboola\Temp\Temp;
use Keboola\Syrup\Aws\S3\Uploader;

class JobExecutor
{
    /**
     * @var SharedStorage
     */
    protected $sharedStorage;
    /**
     * @var RestApi
     */
    protected $restApi;
    /**
     * @var Logger
     */
    protected $logger;
    /**
     * @var Temp
     */
    protected $temp;

    /**
     * @var StorageApiClient
     */
    protected $storageApiClient;
    /**
     * @var \Keboola\GoodDataWriter\Service\Queue
     */
    protected $queue;
    /**
     * @var \Symfony\Component\Translation\TranslatorInterface
     */
    protected $translator;
    /**
     * @var EventLogger
     */
    protected $eventLogger;
    /**
     * @var Uploader
     */
    protected $s3Uploader;
    protected $scriptsPath;
    protected $userAgent;
    protected $gdConfig;


    /**
     *
     */
    public function __construct(
        $scriptsPath,
        $userAgent,
        $gdConfig,
        SharedStorage $sharedStorage,
        RestApi $restApi,
        Logger $logger,
        Temp $temp,
        Queue $queue,
        TranslatorInterface $translator,
        Uploader $s3Uploader,
        S3Client $s3Client
    ) {
        $this->gdConfig = $gdConfig;
        $this->sharedStorage = $sharedStorage;
        $this->restApi = $restApi;
        $this->logger = $logger;
        $this->temp = $temp;
        $this->queue = $queue;
        $this->translator = $translator;
        $this->s3Uploader = $s3Uploader;
        $this->s3Client = $s3Client;
        $this->scriptsPath = $scriptsPath;
        $this->userAgent = $userAgent;
    }

    /**
     * Job execution
     * Performs execution of job tasks and logging
     */
    public function run($jobId, $forceRun = false)
    {
        $this->temp->initRunFolder();

        $startTime = time();

        $jobData = $this->sharedStorage->fetchJob($jobId);
        if (!$jobData) {
            throw new JobProcessException($this->translator->trans('job_executor.job_not_found %1', ['%1' => $jobId]));
        }

        // Job already executed?
        if (!$forceRun && $jobData['status'] != SharedStorage::JOB_STATUS_WAITING) {
            return;
        }

        $queueIdArray = explode('.', $jobData['queueId']);
        $serviceRun = isset($queueIdArray[2]) && $queueIdArray[2] == SharedStorage::SERVICE_QUEUE;

        $jobDataToSave = ['result' => []];
        try {
            $this->storageApiClient = new StorageApiClient([
                'token' => $jobData['token'],
                'userAgent' => $this->userAgent
            ]);
            $this->storageApiClient->setRunId($jobData['runId']);
            $this->eventLogger = new EventLogger($this->storageApiClient, $this->s3Client);

            try {
                $configuration = new Configuration($this->storageApiClient, $this->sharedStorage);
                $configuration->setWriterId($jobData['writerId']);
                $writerInfo = $this->sharedStorage->getWriter($jobData['projectId'], $jobData['writerId']);
                if (!$serviceRun && $writerInfo['status'] == SharedStorage::WRITER_STATUS_MAINTENANCE) {
                    throw new QueueUnavailableException($this->translator->trans('queue.maintenance'));
                }

                $this->sharedStorage->saveJob($jobId, [
                    'status' => SharedStorage::JOB_STATUS_PROCESSING,
                    'startTime' => date('c', $startTime),
                    'endTime' => null
                ]);
                $this->logEvent($this->translator->trans('log.job.started %1', ['%1' => $jobData['id']]), $jobData, [
                    'command' => $jobData['command'],
                    'params' => $jobData['parameters']
                ]);

                $this->restApi->setJobId($jobData['id']);
                $this->restApi->setRunId($jobData['runId']);
                $this->restApi->setEventLogger($this->eventLogger);
                try {
                    $bucketAttributes = $configuration->bucketAttributes();
                    if (!empty($bucketAttributes['gd']['backendUrl'])) {
                        $this->restApi->setBaseUrl($bucketAttributes['gd']['backendUrl']);
                    }
                } catch (WrongConfigurationException $e) {
                    // Ignore
                }

                $jobFactory = new JobFactory(
                    $this->gdConfig,
                    $this->sharedStorage,
                    $configuration,
                    $this->storageApiClient,
                    $this->scriptsPath,
                    $this->eventLogger,
                    $this->translator,
                    $this->temp,
                    $this->logger,
                    $this->s3Client,
                    $this->queue
                );
                $job = $jobFactory->getJobClass($jobData['command']);

                $error = null;

                try {
                    $jobDataToSave['result'] = $job->run($jobData, $jobData['parameters'], $this->restApi);
                } catch (\Exception $e) {
                    $debug = [
                        'code' => $e->getCode(),
                        'message' => $e->getMessage(),
                        'file' => $e->getFile(),
                        'line' => $e->getLine(),
                        'trace' => explode("\n", $e->getTraceAsString())
                    ];

                    if ($e instanceof RestApiException) {
                        $jobDataToSave['result']['error'] = $this->translator->trans('error.rest_api') . '. ' . $e->getMessage();
                        $debug['details'] = $e->getDetails();
                    } elseif ($e instanceof StorageApiClientException) {
                        $jobDataToSave['result']['error'] = $this->translator->trans('error.storage_api') . '. ' . $e->getMessage();
                        if ($e->getPrevious() instanceof CurlException) {
                            /* @var CurlException $curlException */
                            $curlException = $e->getPrevious();
                            $debug['curl'] = $curlException->getCurlInfo();
                        }
                    } elseif ($e instanceof CsvHandlerNetworkException) {
                        $jobDataToSave['result']['error'] = $e->getMessage();
                        $debug['details'] = $e->getData();
                        $this->logger->alert($e->getMessage(), [
                            'job' => $jobData,
                            'exception' => $e,
                            'runId' => $this->storageApiClient->getRunId()
                        ]);
                    } elseif ($e instanceof UserException) {
                        $jobDataToSave['result']['error'] = $e->getMessage();
                        $debug['details'] = $e->getData();
                    } else {
                        throw $e;
                    }

                    $jobDataToSave['debug'] = $this->s3Uploader->uploadString($jobData['id'] . '/debug-data.json', json_encode($debug, JSON_PRETTY_PRINT));
                }

                $jobDataToSave['logs'] = $job->getLogs();

            } catch (\Exception $e) {
                if ($e instanceof QueueUnavailableException) {
                    throw $e;
                } else {
                    $this->logger->alert($e->getMessage(), [
                        'job' => $jobData,
                        'exception' => $e,
                        'runId' => $this->storageApiClient->getRunId()
                    ]);
                    $jobDataToSave['result']['error'] = $this->translator->trans('error.application');
                }
            }
        } catch (StorageApiException $e) {
            $jobDataToSave['result']['error'] = $this->translator->trans('error.storage_api') . ': ' . $e->getMessage();
        }

        $jobDataToSave['status'] = empty($jobDataToSave['result']['error']) ? StorageApiEvent::TYPE_SUCCESS : StorageApiEvent::TYPE_ERROR;
        $jobDataToSave['endTime'] = date('c');

        $this->sharedStorage->saveJob($jobId, $jobDataToSave);

        $log = [
            'command' => $jobData['command'],
            'params' => $jobData['parameters'],
            'status' => $jobDataToSave['status'],
            'result' => $jobDataToSave['result']
        ];
        if (isset($jobDataToSave['debug'])) {
            $log['debug'] = $jobDataToSave['debug'];
        }
        $this->logEvent($this->translator->trans('log.job.finished %1', ['%1' => $jobData['id']]), $jobData, $log, $startTime);
    }


    /**
     * Log event to client SAPI and to system log
     */
    protected function logEvent($message, $job, $data = null, $startTime = null)
    {
        $logData = [
            'jobId' => $job['id'],
            'projectId' => $job['projectId'],
            'writerId' => $job['writerId'],
            'runId' => $job['runId']
        ];
        if ($data) {
            $logData = array_merge($logData, $data);
        }
        array_walk($logData['params'], function(&$val, $key) {
            if ($key == 'password') {
                $val = '***';
            }
        });

        $this->logger->log(Logger::INFO, $message, $logData);

        if (isset($data['debug'])) {
            unset($data['debug']);
        }
        $this->eventLogger->log($job['id'], $job['runId'], $message, $data, time()-$startTime);
    }
}
