<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests\Functional;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Writer\JobFactory;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\Syrup\Service\Queue\QueueFactory;
use Monolog\Handler\NullHandler;
use Symfony\Component\Translation\Translator;
use Keboola\Syrup\Encryption\Encryptor;
use Keboola\Syrup\Aws\S3\Uploader;

class AbstractTest extends \PHPUnit_Framework_TestCase
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
     * @var S3Client
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

    public function setUp()
    {
        parent::setUp();

        $appName = 'gooddata-writer';
        $this->scriptsPath = __DIR__ . '/../GoodData';
        $this->userAgent = 'gooddata-writer (testing)';
        $this->gdConfig = [
            'access_token' => GD_ACCESS_TOKEN,
            'domain' => GD_DOMAIN_NAME,
            'sso_provider' => GD_SSO_PROVIDER
        ];
        $s3Config = [
            'aws-access-key' => '',
            'aws-secret-key' => '',
            's3-upload-path' => '',
            'bitly-login' => '',
            'bitly-api-key' => ''
        ];

        $encryptor = new Encryptor(ENCRYPTION_KEY);

        $db = \Doctrine\DBAL\DriverManager::getConnection([
            'driver' => 'pdo_mysql',
            'host' => DB_HOST,
            'dbname' => DB_NAME,
            'user' => DB_USER,
            'password' => DB_PASSWORD,
        ]);

        $this->sharedStorage = new SharedStorage($db, $encryptor);
        $this->logger = new \Monolog\Logger($appName);
        $this->logger->pushHandler(new NullHandler());
        $this->restApi = new RestApi($appName, $this->logger);
        $this->temp = new \Keboola\Temp\Temp($appName);
        $this->translator = new Translator('en');
        $this->s3uploader = new Uploader($s3Config);
        $this->s3client = new S3Client($s3Config);

        $this->storageApiClient = new StorageApiClient(['token' => STORAGE_API_TOKEN]);
        $this->configuration = new Configuration($this->storageApiClient, $this->sharedStorage);
        $this->configuration->projectId = rand(1, 128);

        $queueFactory = new QueueFactory($db, ['db_table' => 'queues'], SYRUP_APP_NAME);


        //@TODO připravit konfiguraci

        $eventLogger = new EventLogger($this->storageApiClient, $this->s3client);
        $this->jobFactory = new JobFactory($this->gdConfig, $this->scriptsPath);
        $this->jobFactory
            ->setSharedStorage($this->sharedStorage)
            ->setConfiguration($this->configuration)
            ->setStorageApiClient($this->storageApiClient)
            ->setEventLogger($eventLogger)
            ->setTranslator($this->translator)
            ->setTemp($this->temp)
            ->setLogger($this->logger)
            ->setS3Client($this->s3client)
            ->setQueue($queueFactory);


        // Cleanup
        $this->cleanup();
    }

    protected function cleanup()
    {
        $domainUser = $this->sharedStorage->getDomainUser(GD_DOMAIN_NAME);
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

    protected function prepareJobInfo($writerId, $command, $params)
    {
        return [
            'id' => rand(1, 128),
            'batchId' => rand(1, 128),
            'runId' => rand(1, 128),
            'projectId' => rand(1, 128),
            'writerId' => $writerId,
            'token' => STORAGE_API_TOKEN,
            'tokenId' => rand(1, 128),
            'tokenDesc' => uniqid(),
            'createdTime' => date('c'),
            'command' => $command,
            'parameters' => $params
        ];
    }
}
