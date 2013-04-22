<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;
use Keboola\GoodDataWriter\Command\RunJobCommand;

class WriterTest extends WebTestCase
{
	const BUCKET_NAME = 'wr-gooddata-test';
	const BUCKET_ID = 'sys.c-wr-gooddata-test';
	const WRITER_ID = 'test';

	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected static $storageApi;
	/**
	 * @var \Keboola\GoodDataWriter\GoodData\RestApi
	 */
	protected static $restApi;

	public static function setUpBeforeClass()
	{
		$client = static::createClient();
		self::$storageApi = new \Keboola\StorageApi\Client($client->getContainer()->getParameter('storageApi.test.token'),
			$client->getContainer()->getParameter('storageApi.url'));
		self::$restApi = new \Keboola\GoodDataWriter\GoodData\RestApi(null, $client->getContainer()->get('logger'));

		// Clear test environment
		if (self::$storageApi->bucketExists(self::BUCKET_ID)) {
			$bucketInfo = self::$storageApi->getBucket(self::BUCKET_ID);
			foreach ($bucketInfo['tables'] as $table) {
				self::$storageApi->dropTable($table['id']);
			}
			self::$storageApi->dropBucket(self::BUCKET_ID);
		}

	}

	public static function tearDownAfterClass()
	{

	}

	public function testCreateWriter()
	{
		// Send to queue
		$client = static::createClient();
		/*$client->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => $client->getContainer()->getParameter('storageApi.test.token')
		));
		$crawler = $client->request('POST', '/gooddata-writer/writers', array(), array(), array(),
			json_encode(array('writerId' => self::WRITER_ID, 'wait' => 1)));
		$response = $client->getResponse();

		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent());
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('job', $responseJson);*/
		$responseJson['job'] = 1556578;


		// Process job
		$application = new Application($client->getKernel());
		$application->add(new RunJobCommand());

		$command = $application->find('gooddata-writer:run-job');
		$commandTester = new CommandTester($command);
		$commandTester->execute(array('command' => $command->getName(), 'job' => $responseJson['job']));


		// Check result
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');
		$validConfiguration = true;
		try {
			$configuration->checkGoodDataSetup();
		} catch (\Keboola\GoodDataWriter\Exception\WrongConfigurationException $e) {
			$validConfiguration = false;
		}
		$this->assertTrue($validConfiguration);
	}

}
