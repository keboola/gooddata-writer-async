<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase,
	Symfony\Bundle\FrameworkBundle\Console\Application,
	Symfony\Component\Console\Tester\CommandTester;
use Keboola\GoodDataWriter\Command\ExecuteBatchCommand,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;
use Doctrine\Common\Annotations\AnnotationRegistry;

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
	 * @var AppConfiguration
	 */
	protected $appConfiguration;
	protected $domainUser;


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
		$this->httpClient = static::createClient();
		$container = $this->httpClient->getContainer();

		/** To make annotations work here */
		AnnotationRegistry::registerAutoloadNamespaces(array(
			'Sensio\\Bundle\\FrameworkExtraBundle' => '../../vendor/sensio/framework-extra-bundle/'
		));

		$uniqueIndex = uniqid();
		$this->writerId = self::WRITER_ID_PREFIX . $uniqueIndex;
		$this->bucketName = 'wr-gooddata-' . self::WRITER_ID_PREFIX . $uniqueIndex;
		$this->bucketId = 'sys.c-wr-gooddata-' . self::WRITER_ID_PREFIX . $uniqueIndex;
		$this->dataBucketName = self::WRITER_ID_PREFIX . $uniqueIndex;
		$this->dataBucketId = 'out.c-' . self::WRITER_ID_PREFIX . $uniqueIndex;

		if (!$this->storageApiToken)
			$this->storageApiToken = $container->getParameter('storage_api.test.token');


		$this->httpClient->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => $this->storageApiToken
		));

		$this->appConfiguration = $container->get('gooddata_writer.app_configuration');
		$this->storageApi = new StorageApiClient(array(
			'token' => $this->storageApiToken,
			'url' => $container->getParameter('storage_api.url'))
		);

		$sharedConfig = $container->get('gooddata_writer.shared_config');
		$this->domainUser = $sharedConfig->getDomainUser($this->appConfiguration->gd_domain);


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
				/**
				 * @var \Monolog\Logger $logger
				 */
				$logger = $container->get('logger');
				foreach ($this->storageApi->listTables($bucket['id']) as $table) {
					if ($isConfigBucket && $table['id'] == $bucket['id'] . '.projects') {
						try {
							$csv = $this->storageApi->exportTable($table['id']);
							foreach(StorageApiClient::parseCsv($csv) as $project) {
								try {
									$this->restApi->dropProject($project['pid']);
								} catch (RestApiException $e) {}
							}
						} catch (\Exception $e) {
							$logger->alert('Storage API error during writers cleanup', array('exception' => $e));
						}
					} elseif ($isConfigBucket && $table['id'] == $bucket['id'] . '.users') {
						try {
							$csv = $this->storageApi->exportTable($table['id']);
							foreach(StorageApiClient::parseCsv($csv) as $user) {
								try {
									$this->restApi->dropUser($user['uid']);
								} catch (RestApiException $e) {}
							}
						} catch (\Exception $e) {
							$logger->alert('Storage API error during writers cleanup', array('exception' => $e));
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

		$this->configuration = new Configuration($this->storageApi, $this->writerId, $this->appConfiguration->scriptsPath);

		// Init writer
		$this->processJob('/writers', array());

		// Reset configuration
		$this->configuration = new Configuration($this->storageApi, $this->writerId, $this->appConfiguration->scriptsPath);
	}


	/**
	 * Prepare data bucket and it's configuration
	 */
	protected function prepareData()
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
		$this->configuration = new Configuration($this->storageApi, $this->writerId, $this->appConfiguration->scriptsPath);
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
	protected function postWriterApi($url, $params = array())
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
	protected function callWriterApi($url, $method = 'POST', $params = array())
	{
		$this->httpClient->request($method, '/gooddata-writer' . $url, array(), array(), array(), json_encode($params));
		$response = $this->httpClient->getResponse();
		/* @var \Symfony\Component\HttpFoundation\Response $response */

		$this->assertTrue(in_array($response->getStatusCode(), array(200, 202)), sprintf("HTTP status of writer call '%s' should be 200 or 202 but is %s", $url, $response->getStatusCode()));
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
	protected function processJob($url, $params = array(), $method = 'POST')
	{
		$writerId = isset($params['writerId']) ? $params['writerId'] : $this->writerId;

		$params['writerId'] = $writerId;
		$responseJson = $this->callWriterApi($url, $method, $params);
		$this->configuration->clearCache();

		$resultId = null;
		if (isset($responseJson['batch'])) {
			$this->commandTester->execute(array(
				'command' => 'gooddata-writer:execute-batch',
				'batchId' => $responseJson['batch']
			));
			$resultId = $responseJson['batch'];
		} elseif (isset($responseJson['job'])) {
			$responseJson = $this->getWriterApi(sprintf('/jobs?writerId=%s&jobId=%d', $writerId, $responseJson['job']));
			$this->commandTester->execute(array(
				'command' => 'gooddata-writer:execute-batch',
				'batchId' => $responseJson['batchId']
			));
			$resultId = $responseJson['id'];
		} else {
			$this->assertTrue(false, sprintf("Response for writer call '%s' should contain 'job' or 'batch' key.", $url));
		}
		return $resultId;
	}

	protected  function createUser($ssoProvider = null)
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
