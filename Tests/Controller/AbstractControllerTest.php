<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\StorageApi\CachedClient;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\Syrup\Command\JobCommand;
use Keboola\Syrup\Elasticsearch\Search;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\Table as StorageApiTable;
use Symfony\Component\DependencyInjection\ContainerInterface;

abstract class AbstractControllerTest extends WebTestCase
{
    const WRITER_ID_PREFIX = 'test_';

    protected $storageApiToken;
    /**
     * @var \Keboola\StorageApi\Client
     */
    protected $storageApi;
    /**
     * @var \Keboola\GoodDataWriter\GoodData\RestApi
     */
    protected $restApi;
    /**
     * @var Configuration
     */
    protected $configuration;
    /**
     * @var \Symfony\Bundle\FrameworkBundle\Client
     */
    protected $httpClient;
    /**
     * @var CommandTester
     */
    protected $commandTester;
    /**
     * @var SharedStorage
     */
    protected $sharedStorage;
    protected $domainUser;


    protected $bucketName;
    protected $bucketId;
    protected $dataBucketName;
    protected $dataBucketId;
    protected $writerId;
    protected $gdDomain;
    /** @var ContainerInterface */
    protected $container;


    /**
     * Setup called before every test
     */
    protected function setUp()
    {
        $this->httpClient = static::createClient();
        $container = $this->httpClient->getContainer();
        $this->container = $container;

        $uniqueIndex = uniqid();
        $this->writerId = self::WRITER_ID_PREFIX . $uniqueIndex;
        $this->bucketName = 'wr-gooddata-' . self::WRITER_ID_PREFIX . $uniqueIndex;
        $this->bucketId = 'sys.c-wr-gooddata-' . self::WRITER_ID_PREFIX . $uniqueIndex;
        $this->dataBucketName = self::WRITER_ID_PREFIX . $uniqueIndex;
        $this->dataBucketId = 'out.c-' . self::WRITER_ID_PREFIX . $uniqueIndex;

        if (!$this->storageApiToken) {
            $this->storageApiToken = $container->getParameter('storage_api.test.token');
        }


        $this->httpClient->setServerParameters([
            'HTTP_X-StorageApi-Token' => $this->storageApiToken
        ]);

        $this->storageApi = new StorageApiClient([
            'token' => $this->storageApiToken,
            'url' => $container->getParameter('storage_api.url')
        ]);

        $this->sharedStorage = $container->get('gooddata_writer.shared_storage');
        $gdConfig = $config = $container->getParameter('gdwr_gd');
        if (!isset($gdConfig['domain'])) {
            throw new \Exception("Key 'domain' is missing from gd config");
        }
        $this->gdDomain = $gdConfig['domain'];

        $this->domainUser = $this->sharedStorage->getDomainUser($this->gdDomain);


        $this->restApi = $container->get('gooddata_writer.rest_api');

        // Clear test environment
        // Drop data tables from SAPI
        $this->restApi->login($this->domainUser->username, $this->domainUser->password);
        foreach ($this->storageApi->listBuckets() as $bucket) {
            $isConfigBucket = strstr($bucket['id'], 'sys.c-wr-gooddata-' . self::WRITER_ID_PREFIX) !== false;
            $isDataBucket = substr($bucket['id'], 0, 4) == 'out.';

            if ($isConfigBucket) {
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
                /**
                 * @var \Monolog\Logger $logger
                 */
                $logger = $container->get('logger');
                foreach ($this->storageApi->listTables($bucket['id']) as $table) {
                    if ($isConfigBucket && $table['id'] == $bucket['id'] . '.projects') {
                        try {
                            $csv = $this->storageApi->exportTable($table['id']);
                            foreach (StorageApiClient::parseCsv($csv) as $project) {
                                try {
                                    $this->restApi->dropProject($project['pid']);
                                } catch (RestApiException $e) {
                                }
                            }
                        } catch (\Exception $e) {
                            $logger->alert('Storage API error during writers cleanup', ['exception' => $e]);
                        }
                    } elseif ($isConfigBucket && $table['id'] == $bucket['id'] . '.users') {
                        try {
                            $csv = $this->storageApi->exportTable($table['id']);
                            foreach (StorageApiClient::parseCsv($csv) as $user) {
                                try {
                                    $this->restApi->dropUser($user['uid']);
                                } catch (RestApiException $e) {
                                }
                            }
                        } catch (\Exception $e) {
                            $logger->alert('Storage API error during writers cleanup', ['exception' => $e]);
                        }
                    }
                    $this->storageApi->dropTable($table['id']);
                }
                $this->storageApi->dropBucket($bucket['id']);
            }
        }

        // Init job processing
        $application = new Application($this->httpClient->getKernel());
        $application->add(new JobCommand());
        $command = $application->find('syrup:run-job');
        $this->commandTester = new CommandTester($command);


        // Init writer
        $this->processJob('/writers', []);

        // Reset configuration
        $this->configuration = new Configuration(new CachedClient($this->storageApi), $this->sharedStorage);
        $this->configuration->setWriterId($this->writerId);
    }


    /**
     * Prepare data bucket and it's configuration
     */
    protected function prepareData()
    {
        $this->storageApi->createBucket($this->dataBucketName, 'out', 'Writer Test');

        $table = new StorageApiTable($this->storageApi, $this->dataBucketId . '.categories', null, 'id');
        $table->setHeader(['id', 'name']);
        $table->setFromArray([
            ['c1', 'Category 1'],
            ['c2', 'Category 2']
        ]);
        $table->save();

        $table = new StorageApiTable($this->storageApi, $this->dataBucketId . '.products', null, 'id');
        $table->setHeader(['id', 'name', 'price', 'date', 'category']);
        $table->setFromArray([
            ['p1', 'Product 1', '45', '2013-01-01 00:01:59', 'c1'],
            ['p2', 'Product 2', '26', '2013-01-03 11:12:05', 'c2'],
            ['p3', 'Product 3', '112', '2012-10-28 23:07:06', 'c1']
        ]);
        $table->save();



        // Prepare Writer configuration
        $table = new StorageApiTable($this->storageApi, $this->bucketId . '.date_dimensions', null, 'name');
        $table->setHeader($this->configuration->tables[Configuration::DATE_DIMENSIONS_TABLE_NAME]['columns']);
        $table->setFromArray([
            ['ProductDate', 1, 0, '']
        ]);
        $table->save();

        $table = new StorageApiTable($this->storageApi, $this->bucketId . '.data_sets', null, 'id');
        $table->setHeader($this->configuration->tables[Configuration::DATA_SETS_TABLE_NAME]['columns']);
        $table->setFromArray([
            [$this->dataBucketId.'.categories', 'Categories', 1, 0, '', 0, 0, json_encode([
                'id' => [
                    'type' => 'CONNECTION_POINT',
                    'gdName' => 'Id'
                ],
                'name' => [
                    'type' => 'ATTRIBUTE',
                    'gdName' => 'Name'
                ]
            ])],
            [$this->dataBucketId.'.products', 'Products', 1, 0, '', 0, 0, json_encode([
                'id' => [
                    'type' => 'CONNECTION_POINT',
                    'gdName' => 'Id'
                ],
                'name' => [
                    'type' => 'ATTRIBUTE',
                    'gdName' => 'Name'
                ],
                'price' => [
                    'type' => 'FACT',
                    'gdName' => 'Price'
                ],
                'date' => [
                    'type' => 'DATE',
                    'format' => 'yyyy-MM-dd HH:mm:ss',
                    'dateDimension' => 'ProductDate'
                ],
                'category' => [
                    'type' => 'REFERENCE',
                    'gdName' => 'Name',
                    'schemaReference' => $this->dataBucketId . '.categories'
                ]
            ])]
        ]);
        $table->save();

        // Reset configuration
        $this->configuration = new Configuration(new CachedClient($this->storageApi), $this->sharedStorage);
        $this->configuration->setWriterId($this->writerId);
    }


    /**
     * GET request to Writer API
     * @param $url
     * @return mixed
     */
    protected function getWriterApi($url)
    {
        return $this->callWriterApi($url, 'GET');
    }

    /**
     * POST request to Writer API
     * @param $url
     * @param $params
     * @return mixed
     */
    protected function postWriterApi($url, $params = [])
    {
        return $this->callWriterApi($url, 'POST', $params);
    }

    /**
     * Request to Writer API
     * @param $url
     * @param string $method
     * @param array $params
     * @return mixed
     */
    protected function callWriterApi($url, $method = 'POST', $params = [])
    {
        $this->httpClient->request($method, '/gooddata-writer' . $url, [], [], [], json_encode($params));
        $response = $this->httpClient->getResponse();
        /* @var \Symfony\Component\HttpFoundation\Response $response */

        $this->assertGreaterThanOrEqual(200, $response->getStatusCode(), sprintf("HTTP status of writer call '%s' should be greater than or equal to 200 but is %s", $url, $response->getStatusCode()));
        $this->assertLessThan(300, $response->getStatusCode(), sprintf("HTTP status of writer call '%s' should be less than 300 but is %s", $url, $response->getStatusCode()));
        $responseJson = json_decode($response->getContent(), true);
        $this->assertNotEmpty($responseJson, sprintf("Response for writer call '%s' should not be empty.", $url));

        return $responseJson;
    }


    /**
     * Call API and process the job immediately
     * @param $url
     * @param $params
     * @param string $method
     * @return null
     */
    protected function processJob($url, $params = [], $method = 'POST')
    {
        $writerId = isset($params['writerId']) ? $params['writerId'] : $this->writerId;

        $params['writerId'] = $writerId;
        $responseJson = $this->callWriterApi($url, $method, $params);
        if ($this->configuration) {
            $this->configuration->clearCache();
        }

        if (isset($responseJson['id'])) {
            $this->commandTester->execute([
                'command' => 'syrup:run-job',
                'jobId' => $responseJson['id']
            ]);
            return $responseJson['id'];
        } else {
            $this->assertTrue(false, sprintf("Response for writer call '%s' should contain 'id' key.", $url));
        }
        return false;
    }

    protected function getJobFromElasticsearch($id)
    {
        /** @var Search $jobSearch */
        $jobSearch = $this->container->get('gooddata_writer.elasticsearch.search');
        return $jobSearch->getJob($id);
    }

    protected function createUser($ssoProvider = null)
    {
        $params = [
            'email' => 'test' . time() . uniqid() . '@test.keboola.com',
            'password' => md5(uniqid()),
            'firstName' => 'Test',
            'lastName' => 'KBC'
        ];
        if ($ssoProvider) {
            $params['ssoProvider'] = $ssoProvider;
        }
        $this->processJob('/users', $params);

        $usersList = $this->configuration->getUsers();
        $this->assertGreaterThanOrEqual(2, $usersList, "Response for writer call '/users' should return at least two GoodData users.");
        return $usersList[count($usersList)-1];
    }

    protected function getAttributes($pid)
    {
        $query = sprintf('/gdc/md/%s/query/attributes', $pid);

        $result = $this->getWriterApi('/proxy?writerId=' . $this->writerId . '&query=' . $query);

        if (isset($result['response']['query']['entries'])) {
            return $result['response']['query']['entries'];
        } else {
            throw new \Exception('Attributes in project could not be fetched');
        }
    }

    protected function getAttributeByTitle($pid, $title)
    {
        foreach ($this->getAttributes($pid) as $attr) {
            if ($attr['title'] == $title) {
                $result = $this->getWriterApi('/proxy?writerId=' . $this->writerId . '&query=' . $attr['link']);
                return $result['response'];
            }
        }
        return false;
    }
}
