<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests\Unit\Task;

use Elasticsearch\Client;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\Job\Metadata\JobFactory;
use Keboola\GoodDataWriter\StorageApi\CachedClient;
use Keboola\GoodDataWriter\StorageApi\EventLogger;
use Keboola\GoodDataWriter\Aws\S3Client;
use Keboola\GoodDataWriter\Task\Factory;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\Syrup\Elasticsearch\ComponentIndex;
use Keboola\Syrup\Elasticsearch\JobMapper;
use Keboola\Syrup\Elasticsearch\Search;
use Keboola\Syrup\Service\Queue\QueueFactory;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Symfony\Component\Translation\Translator;
use Keboola\Syrup\Encryption\Encryptor;
use Keboola\Syrup\Aws\S3\Uploader;

abstract class AbstractTaskTest extends \PHPUnit_Framework_TestCase
{

    /**
     * @var SharedStorage
     */
    protected $sharedStorage;
    /**
     * @var \Monolog\Logger
     */
    protected $logger;
    /**
     * @var \Keboola\Temp\Temp
     */
    protected $temp;
    /**
     * @var Translator
     */
    protected $translator;
    /**
     * @var \Keboola\GoodDataWriter\Aws\S3Client
     */
    protected $s3client;
    /**
     * @var Uploader
     */
    protected $s3uploader;

    /**
     * @var RestApi
     */
    protected $restApi;

    protected $scriptsPath;
    protected $userAgent;
    protected $gdConfig;
    /**
     * @var StorageApiClient
     */
    protected $storageApiClient;
    /**
     * @var Configuration
     */
    protected $configuration;
    /**
     * @var JobFactory
     */
    protected $jobFactory;
    /**
     * @var Factory
     */
    protected $taskFactory;

    public function setUp()
    {
        parent::setUp();

        $this->scriptsPath = __DIR__ . '/../GoodData';
        $this->userAgent = 'gooddata-writer (testing)';
        $this->gdConfig = [
            'access_token' => GW_GD_ACCESS_TOKEN,
            'domain' => GW_GD_DOMAIN_NAME,
            'sso_provider' => GW_GD_SSO_PROVIDER
        ];
        $s3Config = [
            'aws-access-key' => GW_AWS_ACCESS_KEY,
            'aws-secret-key' => GW_AWS_SECRET_KEY,
            's3-upload-path' => GW_AWS_S3_BUCKET
        ];

        $encryptor = new Encryptor(GW_ENCRYPTION_KEY);

        $db = \Doctrine\DBAL\DriverManager::getConnection([
            'driver' => 'pdo_mysql',
            'host' => GW_DB_HOST,
            'dbname' => GW_DB_NAME,
            'user' => GW_DB_USER,
            'password' => GW_DB_PASSWORD,
        ]);

        $this->sharedStorage = new SharedStorage($db, $encryptor);
        $this->logger = new \Monolog\Logger(GW_APP_NAME);
        $this->logger->pushHandler(new StreamHandler(STDERR, Logger::ALERT));
        $this->restApi = new RestApi(GW_APP_NAME, $this->logger);
        $this->temp = new \Keboola\Temp\Temp(GW_APP_NAME);
        $this->translator = new Translator('en');
        $this->s3uploader = new Uploader($s3Config);
        $this->s3client = new S3Client($s3Config);

        $this->storageApiClient = new StorageApiClient(['token' => GW_STORAGE_API_TOKEN]);
        $this->configuration = new Configuration(new CachedClient($this->storageApiClient), $this->sharedStorage);
        $this->configuration->projectId = rand(1, 128);
        $this->configuration->writerId = uniqid();

        $queueFactory = new QueueFactory($db, ['db_table' => 'queues'], GW_APP_NAME);
        $elasticsearchClient = new Client(['hosts' => [GW_ELASTICSEARCH_HOST]]);
        $jobMapper = new JobMapper(
            $elasticsearchClient,
            new ComponentIndex(GW_APP_NAME, 'devel', $elasticsearchClient)
        );
        $syrupJobFactory = new \Keboola\Syrup\Job\Metadata\JobFactory(GW_APP_NAME, $encryptor);
        $syrupJobFactory->setStorageApiClient($this->storageApiClient);
        $jobSearch = new \Keboola\GoodDataWriter\Elasticsearch\Search($elasticsearchClient, 'devel', GW_APP_NAME);


        //@TODO připravit konfiguraci

        $eventLogger = new EventLogger($this->storageApiClient, $this->s3client);
        $this->jobFactory = new JobFactory($queueFactory, $jobMapper, $syrupJobFactory, $jobSearch);
        $this->jobFactory
            ->setStorageApiClient($this->storageApiClient)
            ->setConfiguration($this->configuration);
        $this->taskFactory = new Factory(
            $this->gdConfig,
            $this->scriptsPath,
            $this->sharedStorage,
            $this->restApi,
            $this->jobFactory,
            $this->s3client,
            $this->temp,
            $this->translator,
            $this->logger,
            new Search($elasticsearchClient, 'devel')
        );
        $this->taskFactory
            ->setEventLogger($eventLogger)
            ->setStorageApiClient($this->storageApiClient)
            ->setConfiguration($this->configuration);

        // Cleanup
        $this->cleanup();
    }

    protected function cleanup()
    {
        $domainUser = $this->sharedStorage->getDomainUser(GW_GD_DOMAIN_NAME);
        $this->restApi->login($domainUser->username, $domainUser->password);
        foreach ($this->storageApiClient->listBuckets() as $bucket) {
            $isConfigBucket = strstr($bucket['id'], 'sys.c-wr-gooddata-') !== false;
            $isDataBucket = substr($bucket['id'], 0, 4) == 'out.';

            if ($isConfigBucket) {
                // Drop main GD project and GD user
                foreach ($bucket['attributes'] as $attr) {
                    if ($attr['name'] == 'gd.pid') {
                        try {
                            $this->restApi->dropProject($attr['value']);
                        } catch (RestApiException $e) {
                        }
                    }
                    if ($attr['name'] == 'gd.uid') {
                        try {
                            $this->restApi->dropUser($attr['value']);
                        } catch (RestApiException $e) {
                        }
                    }
                }
            }

            if ($isConfigBucket || $isDataBucket) {
                foreach ($this->storageApiClient->listTables($bucket['id']) as $table) {
                    if ($isConfigBucket && $table['id'] == $bucket['id'] . '.projects') {
                        // Drop cloned GD projects
                        try {
                            $csv = $this->storageApiClient->exportTable($table['id']);
                            foreach (StorageApiClient::parseCsv($csv) as $project) {
                                try {
                                    $this->restApi->dropProject($project['pid']);
                                } catch (RestApiException $e) {
                                }
                            }
                        } catch (\Exception $e) {
                        }
                    } elseif ($isConfigBucket && $table['id'] == $bucket['id'] . '.users') {
                        // Drop GD users
                        try {
                            $csv = $this->storageApiClient->exportTable($table['id']);
                            foreach (StorageApiClient::parseCsv($csv) as $user) {
                                try {
                                    $this->restApi->dropUser($user['uid']);
                                } catch (RestApiException $e) {
                                }
                            }
                        } catch (\Exception $e) {
                        }
                    }

                    $this->storageApiClient->dropTable($table['id']);
                }
                $this->storageApiClient->dropBucket($bucket['id']);
            }
        }
    }

    protected function createJob()
    {
        return new Job([
            'id' => $this->storageApiClient->generateId(),
            'runId' => $this->storageApiClient->generateId(),
            'project' => [
                'id' => '123',
                'name' => 'Syrup TEST'
            ],
            'token' => [
                'id' => '123',
                'description' => 'fake token',
                'token' => uniqid()
            ],
            'component' => 'syrup',
            'command' => 'run',
            'params' => [],
            'process' => [
                'host' => gethostname(),
                'pid' => getmypid()
            ],
            'createdTime' => date('c')
        ]);
    }
}
