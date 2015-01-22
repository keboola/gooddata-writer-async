<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\Tests\Unit;

use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Client;
use Syrup\ComponentBundle\Encryption\Encryptor;
use Keboola\StorageApi\Table as StorageApiTable;

class ConfigurationTest extends \PHPUnit_Framework_TestCase
{

	/**
	 * @var Configuration $configuration
	 */
	private $configuration;
	/**
	 * @var Client
	 */
	private $storageApiClient;

	public function setUp()
	{
		$db = \Doctrine\DBAL\DriverManager::getConnection(array(
			'driver' => 'pdo_mysql',
			'host' => DB_HOST,
			'dbname' => DB_NAME,
			'user' => DB_USER,
			'password' => DB_PASSWORD,
		));
		$sharedStorage = new SharedStorage($db, new Encryptor(ENCRYPTION_KEY));

		$this->storageApiClient = new Client(array('token' => STORAGE_API_TOKEN, 'url' => STORAGE_API_URL));
		$this->configuration = new Configuration($this->storageApiClient, $sharedStorage);

		// Cleanup
		foreach ($this->storageApiClient->listBuckets() as $bucket) {
			if ($bucket['id'] != 'sys.logs') {
				foreach ($this->storageApiClient->listTables($bucket['id']) as $table) {
					$this->storageApiClient->dropTable($table['id']);
				}
				$this->storageApiClient->dropBucket($bucket['id']);
			}
		}

		// Create test config
		$dataBucketName = uniqid();
		$dataBucketId = $this->storageApiClient->createBucket($dataBucketName, 'out', 'Writer Test');

		$table = new StorageApiTable($this->storageApiClient, $dataBucketId . '.categories', null, 'id');
		$table->setHeader(array('id', 'name'));
		$table->setFromArray(array(
			array('c1', 'Category 1'),
			array('c2', 'Category 2')
		));
		$table->save();

		$table = new StorageApiTable($this->storageApiClient, $dataBucketId . '.products', null, 'id');
		$table->setHeader(array('id', 'name', 'price', 'date', 'category'));
		$table->setFromArray(array(
			array('p1', 'Product 1', '45', '2013-01-01 00:01:59', 'c1'),
			array('p2', 'Product 2', '26', '2013-01-03 11:12:05', 'c2'),
			array('p3', 'Product 3', '112', '2012-10-28 23:07:06', 'c1')
		));
		$table->save();
	}

	public function testWriterConfiguration()
	{
		$writerId = '' . uniqid();

		// setWriterId()
		$this->configuration->setWriterId($writerId);
		$this->assertEquals($writerId, $this->configuration->writerId);

		// createBucket(), bucketId
		$configBucketId = 'sys.c-wr-gooddata-' . $writerId;
		$this->configuration->createBucket($writerId);
		$this->assertEquals($configBucketId, $this->configuration->bucketId);
		$this->assertTrue($this->storageApiClient->bucketExists($configBucketId));
		$bucket = $this->storageApiClient->getBucket('sys.c-wr-gooddata-' . $writerId);
		$writerAttr = false;
		$writerIdAttr = false;
		foreach ($bucket['attributes'] as $attr) {
			if ($attr['name'] == 'writer') {
				if ($attr['value'] == 'gooddata') {
					$writerAttr = true;
				}
			}
			if ($attr['name'] == 'writerId') {
				if ($attr['value'] == $writerId) {
					$writerIdAttr = true;
				}
			}
		}
		$this->assertTrue($writerAttr);
		$this->assertTrue($writerIdAttr);

		// updateBucketAttribute()
		$testAttrName = uniqid();
		$this->configuration->updateBucketAttribute($testAttrName, $testAttrName);

		// getWriterToApi()
		$writer = $this->configuration->getWriterToApi();
		$this->assertEquals($writerId, $writer['writerId']);
		$this->assertEquals('error', $writer['status']);
		$this->assertArrayHasKey($testAttrName, $writer);
		$this->assertEquals($testAttrName, $writer[$testAttrName]);

		// getWritersToApi()
		$writers = $this->configuration->getWritersToApi();
		$this->assertCount(1, $writers);
		$writer = current($writers);
		$this->assertEquals($writerId, $writer['writerId']);
		$this->assertEquals('error', $writer['status']);
		$this->assertArrayHasKey($testAttrName, $writer);
		$this->assertEquals($testAttrName, $writer[$testAttrName]);

		// deleteBucket()
		$this->assertTrue($this->storageApiClient->bucketExists($configBucketId));
		$this->configuration->deleteBucket();
		$this->assertFalse($this->storageApiClient->bucketExists($configBucketId));
	}

}