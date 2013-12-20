<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\GoodData\RestApiException;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase,
	Symfony\Bundle\FrameworkBundle\Console\Application,
	Symfony\Component\Console\Tester\CommandTester;
use Keboola\GoodDataWriter\Command\ExecuteBatchCommand,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;

abstract class AbstractControllerTest extends WebTestCase
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $storageApi;
	/**
	 * @var RestApi
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
	protected $mainConfig;


	protected $bucketName;
	protected $bucketId;
	protected $dataBucketName;
	protected $dataBucketId;
	protected $writerId;


	/**
	 * Setup called before every test
	 */
	protected function setUp()
	{
		$uniqueIndex = uniqid();
		$this->writerId = 'test' . $uniqueIndex;
		$this->bucketName = 'wr-gooddata-test' . $uniqueIndex;
		$this->bucketId = 'sys.c-wr-gooddata-test' . $uniqueIndex;
		$this->dataBucketName = 'test' . $uniqueIndex;
		$this->dataBucketId = 'out.c-test' . $uniqueIndex;


		$this->httpClient = static::createClient();
		$container = $this->httpClient->getContainer();
		$this->httpClient->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => $container->getParameter('storageApi.test.token')
		));

		$this->mainConfig = $container->getParameter('gooddata_writer');
		$this->storageApi = new \Keboola\StorageApi\Client($container->getParameter('storageApi.test.token'),
			$this->httpClient->getContainer()->getParameter('storageApi.test.url'));
		$this->restApi = new RestApi($container->get('logger'));

		// Clear test environment
		// Drop data tables from SAPI
		$this->restApi->setCredentials($this->mainConfig['gd']['username'], $this->mainConfig['gd']['password']);
		foreach ($this->storageApi->listBuckets() as $bucket) {
			$isConfigBucket = substr($bucket['id'], 0, 22) == 'sys.c-wr-gooddata-test';
			$isDataBucket = substr($bucket['id'], 0, 4) == 'out.';

			if ($isConfigBucket) {
				foreach ($bucket['attributes'] as $attr) {
					if ($attr['name'] == 'gd.pid') {
						try {
							$this->restApi->dropProject($attr['value']);
						} catch (RestApiException $e) {}
					}
					if ($attr['name'] == 'gd.uid') {
						try {
							$this->restApi->dropUser($attr['value']);
						} catch (RestApiException $e) {}
					}
				}
			}

			if ($isConfigBucket || $isDataBucket) {
				foreach ($this->storageApi->listTables($bucket['id']) as $table) {
					if ($isConfigBucket && $table['id'] == $bucket['id'] . '.projects') {
						$csv = $this->storageApi->exportTable($table['id']);
						foreach(StorageApiClient::parseCsv($csv) as $project) {
							try {
								$this->restApi->dropProject($project['pid']);
							} catch (RestApiException $e) {}
						}
					} elseif ($isConfigBucket && $table['id'] == $bucket['id'] . '.users') {
						$csv = $this->storageApi->exportTable($table['id']);
						foreach(StorageApiClient::parseCsv($csv) as $user) {
							try {
								$this->restApi->dropUser($user['uid']);
							} catch (RestApiException $e) {}
						}
					}
					$this->storageApi->dropTable($table['id']);
				}
				$this->storageApi->dropBucket($bucket['id']);
			}
		}

		// Init job processing
		$application = new Application($this->httpClient->getKernel());
		$application->add(new ExecuteBatchCommand());
		$command = $application->find('gooddata-writer:execute-batch');
		$this->commandTester = new CommandTester($command);

		$this->configuration = new Configuration($this->storageApi, $this->writerId);

		// Init writer
		$this->_processJob('/gooddata-writer/writers', array());

		// Reset configuration
		$this->configuration = new Configuration($this->storageApi, $this->writerId);
	}


	/**
	 * Prepare data bucket and it's configuration
	 */
	protected function _prepareData()
	{
		$this->storageApi->createBucket($this->dataBucketName, 'out', 'Writer Test');

		$table = new StorageApiTable($this->storageApi, $this->dataBucketId . '.categories', null, 'id');
		$table->setHeader(array('id', 'name'));
		$table->setFromArray(array(
			array('c1', 'Category 1'),
			array('c2', 'Category 2')
		));
		$table->save();

		$table = new StorageApiTable($this->storageApi, $this->dataBucketId . '.products', null, 'id');
		$table->setHeader(array('id', 'name', 'price', 'date', 'category'));
		$table->setFromArray(array(
			array('p1', 'Product 1', '45', '2013-01-01 00:01:59', 'c1'),
			array('p2', 'Product 2', '26', '2013-01-03 11:12:05', 'c2'),
			array('p3', 'Product 3', '112', '2012-10-28 23:07:06', 'c1')
		));
		$table->save();



		// Prepare Writer configuration
		$this->configuration->saveDateDimension('ProductDate', true);

		$this->configuration->updateDataSetDefinition($this->dataBucketId . '.categories', 'name', 'Categories');
		$this->configuration->updateDataSetDefinition($this->dataBucketId . '.categories', 'export', '1');
		$this->configuration->updateColumnsDefinition($this->dataBucketId . '.categories', array(
			array(
				'name' => 'id',
				'gdName' => 'Id',
				'type' => 'CONNECTION_POINT'
			),
			array(
				'name' => 'name',
				'gdName' => 'Name',
				'type' => 'ATTRIBUTE'
			)
		));

		$this->configuration->updateDataSetDefinition($this->dataBucketId . '.products', 'name', 'Products');
		$this->configuration->updateDataSetDefinition($this->dataBucketId . '.products', 'export', '1');
		$this->configuration->updateColumnsDefinition($this->dataBucketId . '.products', array(
			array(
				'name' => 'id',
				'gdName' => 'Id',
				'type' => 'CONNECTION_POINT'
			),
			array(
				'name' => 'name',
				'gdName' => 'Name',
				'type' => 'ATTRIBUTE'
			),
			array(
				'name' => 'price',
				'gdName' => 'Price',
				'type' => 'FACT'
			),
			array(
				'name' => 'date',
				'gdName' => '',
				'type' => 'DATE',
				'format' => 'yyyy-MM-dd HH:mm:ss',
				'dateDimension' => 'ProductDate'
			),
			array(
				'name' => 'category',
				'gdName' => '',
				'type' => 'REFERENCE',
				'schemaReference' => $this->dataBucketId . '.categories'
			)
		));

		// Reset configuration
		$this->configuration = new Configuration($this->storageApi, $this->writerId);
	}


	/**
	 * GET request to Writer API
	 * @param $url
	 * @return mixed
	 */
	protected function _getWriterApi($url)
	{
		return $this->_callWriterApi($url, 'GET');
	}

	/**
	 * POST request to Writer API
	 * @param $url
	 * @param $params
	 * @return mixed
	 */
	protected function _postWriterApi($url, $params = array())
	{
		return $this->_callWriterApi($url, 'POST', $params);
	}

	/**
	 * Request to Writer API
	 * @param $url
	 * @param string $method
	 * @param array $params
	 * @return mixed
	 */
	protected function _callWriterApi($url, $method = 'POST', $params = array())
	{
		$this->httpClient->request($method, $url, array(), array(), array(), json_encode($params));
		$response = $this->httpClient->getResponse();
		/* @var \Symfony\Component\HttpFoundation\Response $response */

		$this->assertEquals(200, $response->getStatusCode(), sprintf("HTTP status of writer call '%s' should be 200. Response: %s", $url, $response->getContent()));
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
	protected function _processJob($url, $params = array(), $method = 'POST')
	{
		$writerId = isset($params['writerId']) ? $params['writerId'] : $this->writerId;

		$params['writerId'] = $writerId;
		$responseJson = $this->_callWriterApi($url, $method, $params);
		$this->configuration->clearCache();

		$resultId = null;
		if (isset($responseJson['job'])) {
			$responseJson = $this->_getWriterApi(sprintf('/gooddata-writer/jobs?writerId=%s&jobId=%d', $writerId, $responseJson['job']));

			$this->commandTester->execute(array(
				'command' => 'gooddata-writer:execute-batch',
				'batchId' => $responseJson['job']['batchId']
			));
			$resultId = $responseJson['job']['id'];
		} else if (isset($responseJson['batch'])) {
			$this->commandTester->execute(array(
				'command' => 'gooddata-writer:execute-batch',
				'batchId' => $responseJson['batch']
			));
			$resultId = $responseJson['batch'];
		} else {
			$this->assertTrue(false, sprintf("Response for writer call '%s' should contain 'job' or 'batch' key.", $url));
		}
		return $resultId;
	}

	protected  function _createUser($ssoProvider = null)
	{
		$params = array(
			'email' => 'test' . time() . uniqid() . '@test.keboola.com',
			'password' => md5(uniqid()),
			'firstName' => 'Test',
			'lastName' => 'KBC'
		);
		if ($ssoProvider) {
			$params['ssoProvider'] = $ssoProvider;
		}
		$this->_processJob('/gooddata-writer/users', $params);

		$usersList = $this->configuration->getUsers();
		$this->assertGreaterThanOrEqual(2, $usersList, "Response for writer call '/users' should return at least two GoodData users.");
		return $usersList[count($usersList)-1];
	}
}
