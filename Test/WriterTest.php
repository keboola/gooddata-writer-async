<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Test;

use Keboola\GoodDataWriter\GoodData\RestApiException;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase,
	Symfony\Bundle\FrameworkBundle\Console\Application,
	Symfony\Bundle\FrameworkBundle\Client,
	Symfony\Component\Console\Tester\CommandTester;
use Keboola\GoodDataWriter\Command\RunJobCommand,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\StorageApi\Table as StorageApiTable;

abstract class WriterTest extends WebTestCase
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected static $storageApi;
	/**
	 * @var RestApi
	 */
	protected static $restApi;
	/**
	 * @var Configuration
	 */
	protected static $configuration;
	/**
	 * @var \Symfony\Bundle\FrameworkBundle\Client
	 */
	protected static $client;
	/**
	 * @var CommandTester
	 */
	protected static $commandTester;
	protected static $mainConfig;


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


		self::$client = static::createClient();
		$container = self::$client->getContainer();
		self::$client->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => $container->getParameter('storageApi.test.token')
		));

		self::$mainConfig = $container->getParameter('gooddata_writer');
		self::$storageApi = new \Keboola\StorageApi\Client($container->getParameter('storageApi.test.token'),
			self::$client->getContainer()->getParameter('storageApi.url'));
		self::$restApi = new RestApi(null, $container->get('logger'));
		self::$configuration = new Configuration($this->writerId, self::$storageApi, self::$mainConfig['tmp_path']);

		$mainConfig = self::$mainConfig['gd']['dev'];

		// Clear test environment
		if (self::$storageApi->bucketExists($this->bucketId)) {

			self::$restApi->login($mainConfig['username'], $mainConfig['password']);

			// Drop all projects from GD
			foreach (self::$configuration->getProjects() as $p) {
				try {
					self::$restApi->dropProject($p['pid']);
				} catch (RestApiException $e) {}
			}

			// Drop all users from GD
			foreach (self::$configuration->getUsers() as $u) {
				try {
					self::$restApi->dropUser($u['uid']);
				} catch (RestApiException $e) {}
			}

			// Drop configuration from SAPI
			foreach (self::$storageApi->listTables($this->bucketId) as $table) {
				self::$storageApi->dropTable($table['id']);
			}
			self::$storageApi->dropBucket($this->bucketId);
		}

		// Drop data tables from SAPI
		foreach (self::$storageApi->listBuckets() as $bucket) if (substr($bucket['id'], 0, 22) == 'sys.c-wr-gooddata-test' || substr($bucket['id'], 0, 4) == 'out.') {
			foreach (self::$storageApi->listTables($bucket['id']) as $table) {
				self::$storageApi->dropTable($table['id']);
			}
			self::$storageApi->dropBucket($bucket['id']);
		}

		// Init job processing
		$application = new Application(self::$client->getKernel());
		$application->add(new RunJobCommand());
		$command = $application->find('gooddata-writer:run-job');
		self::$commandTester = new CommandTester($command);

		// Init writer
		$this->_processJob('/gooddata-writer/writers', array());
		self::$configuration = new Configuration($this->writerId, self::$storageApi, self::$mainConfig['tmp_path']);
	}


	/**
	 * Prepare data bucket and it's configuration
	 */
	protected function _prepareData()
	{
		// Prepare data
		self::$storageApi->createBucket($this->dataBucketName, 'out', 'Writer Test');

		$table = new StorageApiTable(self::$storageApi, $this->dataBucketId . '.categories', null, 'id');
		$table->setHeader(array('id', 'name'));
		$table->setFromArray(array(
			array('c1', 'Category 1'),
			array('c2', 'Category 2')
		));
		$table->save();

		$table = new StorageApiTable(self::$storageApi, $this->dataBucketId . '.products', null, 'id');
		$table->setHeader(array('id', 'name', 'price', 'date', 'category'));
		$table->setFromArray(array(
			array('p1', 'Product 1', '45', '2013-01-01', 'c1'),
			array('p2', 'Product 2', '26', '2013-01-03', 'c2'),
			array('p3', 'Product 3', '112', '2013-01-03', 'c1')
		));
		$table->save();

		// Prepare Writer configuration
		self::$configuration->addDateDimension('ProductDate', false);

		$table = new StorageApiTable(self::$storageApi, $this->bucketId . '.c-' . $this->dataBucketName . '_categories', null, 'name');
		$table->setAttribute('tableId', $this->dataBucketId . '.categories');
		$table->setAttribute('gdName', 'Categories');
		$table->setAttribute('export', '1');
		$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
			'format', 'dateDimension', 'sortLabel', 'sortOrder'));
		$table->setFromArray(array(
			array('id', 'Id', 'CONNECTION_POINT', '', '', '', '', '', '', '', ''),
			array('name', 'Name', 'ATTRIBUTE', '', '', '', '', '', '', '', '')
		));
		$table->save();

		$table = new StorageApiTable(self::$storageApi, $this->bucketId . '.c-' . $this->dataBucketName . '_products', null, 'name');
		$table->setAttribute('tableId', $this->dataBucketId . '.products');
		$table->setAttribute('gdName', 'Products');
		$table->setAttribute('export', '1');
		$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
			'format', 'dateDimension', 'sortLabel', 'sortOrder'));
		$table->setFromArray(array(
			array('id', 'Id', 'CONNECTION_POINT', '', '', '', '', '', '', '', ''),
			array('name', 'Name', 'ATTRIBUTE', '', '', '', '', '', '', '', ''),
			array('price', 'Price', 'FACT', '', '', '', '', '', '', '', ''),
			array('date', 'Date', 'DATE', '', '', '', '', 'yyyy-MM-dd', 'ProductDate', '', ''),
			array('category', 'Category', 'REFERENCE', '', '', $this->dataBucketId . '.categories', '', '', '', '', '')
		));
		$table->save();
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
	protected function _postWriterApi($url, $params)
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
		self::$client->request($method, $url, array(), array(), array(), json_encode($params));
		$response = self::$client->getResponse();
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
	 */
	protected function _processJob($url, $params = array(), $method = 'POST')
	{
		$responseJson = $this->_callWriterApi($url, $method, array_merge($params, array(
			'writerId' => $this->writerId,
			'dev' => 1
		)));

		if (isset($responseJson['job'])) {
			self::$commandTester->execute(array(
				'command' => 'gooddata-writer:run-job',
				'job' => $responseJson['job']
			));
		} else if (isset($responseJson['batch'])) {
			$responseJson = $this->_getWriterApi(sprintf('/gooddata-writer/batch?writerId=%s&batchId=%d', $this->writerId, $responseJson['batch']));

			$this->assertArrayHasKey('batch', $responseJson, "Response for writer call '/batch' should contain 'batch' key.");
			$this->assertArrayHasKey('jobs', $responseJson['batch'], "Response for writer call '/batch' should contain 'batch.jobs' key.");
			foreach ($responseJson['batch']['jobs'] as $job) {
				self::$commandTester->execute(array(
					'command' => 'gooddata-writer:run-job',
					'job' => $job
				));
			}
		} else {
			$this->assertTrue(false, sprintf("Response for writer call '%s' should contain 'job' or 'batch' key.", $url));
		}
	}

	protected  function _createUser()
	{
		$this->_processJob('/gooddata-writer/users', array(
			'email' => 'test' . time() . uniqid() . '@test.keboola.com',
			'password' => md5(uniqid()),
			'firstName' => 'Test',
			'lastName' => 'KBC'
		));

		$usersList = self::$configuration->getUsers();
		$this->assertGreaterThanOrEqual(2, $usersList, "Response for writer call '/users' should return at least two GoodData users.");
		return $usersList[count($usersList)-1];
	}
}
