<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\Tests\Unit;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
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
	private $dataBucketId;

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
		$this->dataBucketId = $this->storageApiClient->createBucket($dataBucketName, 'out', 'Writer Test');

		$table = new StorageApiTable($this->storageApiClient, $this->dataBucketId . '.categories', null, 'id');
		$table->setHeader(array('id', 'name'));
		$table->setFromArray(array(
			array('c1', 'Category 1'),
			array('c2', 'Category 2')
		));
		$table->save();

		$table = new StorageApiTable($this->storageApiClient, $this->dataBucketId . '.products', null, 'id');
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

	public function testSapiTablesConfiguration()
	{
		// getOutputSapiTables()
		$outputTables = $this->configuration->getOutputSapiTables();
		$this->assertTrue(in_array($this->dataBucketId . '.categories', $outputTables));
		$this->assertTrue(in_array($this->dataBucketId . '.products', $outputTables));

		// getSapiTable()
		$table = $this->configuration->getSapiTable($this->dataBucketId . '.categories');
		$this->assertArrayHasKey('id', $table);
		$this->assertArrayHasKey('uri', $table);

		// getTableIdFromAttribute()
		$this->assertEquals($this->dataBucketId . '.categories', $this->configuration->getTableIdFromAttribute($this->dataBucketId . '.categories.id'));
	}

	public function testUsersConfiguration()
	{
		$writerId = uniqid();
		$this->configuration->setWriterId($writerId);
		$this->configuration->createBucket($writerId);
		$configBucketId = 'sys.c-wr-gooddata-' . $writerId;

		// saveUser()
		$id = uniqid();
		$this->configuration->saveUser($id . '@keboola.com', $id);
		$this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.users'));
		$rawData = $this->storageApiClient->exportTable($configBucketId . '.users');
		$data = Client::parseCsv($rawData);
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('email', $data[0]);
		$this->assertArrayHasKey('uid', $data[0]);
		$this->assertEquals($id . '@keboola.com', $data[0]['email']);
		$this->assertEquals($id, $data[0]['uid']);

		// getUser()
		$user = $this->configuration->getUser($id . '@keboola.com');
		$this->assertArrayHasKey('email', $user);
		$this->assertArrayHasKey('uid', $user);
		$this->assertEquals($id . '@keboola.com', $user['email']);
		$this->assertEquals($id, $user['uid']);

		// getUsers()
		$users = $this->configuration->getUsers();
		$this->assertCount(1, $users);
		$this->assertArrayHasKey('email', $users[0]);
		$this->assertArrayHasKey('uid', $users[0]);
		$this->assertEquals($id . '@keboola.com', $users[0]['email']);
		$this->assertEquals($id, $users[0]['uid']);
		$this->configuration->saveUser(uniqid() . '@keboola.com', uniqid());
		$users = $this->configuration->getUsers();
		$this->assertCount(2, $users);

		// checkUsersTable()
		try {
			$this->configuration->checkUsersTable();
		} catch (WrongConfigurationException $e) {
			$this->fail();
		}
		$this->storageApiClient->deleteTableColumn($configBucketId . '.users', 'email');
		$this->configuration->clearCache();
		try {
			$this->configuration->checkUsersTable();
			$this->fail();
		} catch (WrongConfigurationException $e) {
		}
	}

	public function testProjectsConfiguration()
	{
		$writerId = uniqid();
		$this->configuration->setWriterId($writerId);
		$this->configuration->createBucket($writerId);
		$configBucketId = 'sys.c-wr-gooddata-' . $writerId;

		$mainPid = uniqid();
		$this->configuration->updateBucketAttribute('gd.pid', $mainPid);

		// saveProject()
		$pid = uniqid();
		$this->configuration->saveProject($pid);
		$this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.projects'));
		$rawData = $this->storageApiClient->exportTable($configBucketId . '.projects');
		$data = Client::parseCsv($rawData);
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('pid', $data[0]);
		$this->assertArrayHasKey('active', $data[0]);
		$this->assertEquals($pid, $data[0]['pid']);
		$this->assertEquals(1, $data[0]['active']);

		// getProject()
		$project = $this->configuration->getProject($pid);
		$this->assertArrayHasKey('pid', $project);
		$this->assertArrayHasKey('active', $project);
		$this->assertEquals($pid, $project['pid']);
		$this->assertEquals(1, $project['active']);

		// getProjects()
		$projects = $this->configuration->getProjects();
		$this->assertCount(2, $projects);
		$projectFound = false;
		$mainProjectFound = false;
		foreach ($projects as $project) {
			$this->assertArrayHasKey('pid', $project);
			$this->assertArrayHasKey('active', $project);
			if ($project['pid'] == $pid) {
				$projectFound = true;
			}
			if ($project['pid'] == $mainPid) {
				$mainProjectFound = true;
				$this->assertArrayHasKey('main', $project);
				$this->assertTrue($project['main']);
			}
		}
		$this->assertTrue($mainProjectFound);
		$this->assertTrue($projectFound);
		$this->configuration->saveProject(uniqid());
		$projects = $this->configuration->getProjects();
		$this->assertCount(3, $projects);

		// resetProjectsTable()
		$this->configuration->resetProjectsTable();
		$projects = $this->configuration->getProjects();
		$this->assertCount(1, $projects);

		// checkProjectsTable()
		try {
			$this->configuration->checkProjectsTable();
		} catch (WrongConfigurationException $e) {
			$this->fail();
		}
		$this->storageApiClient->deleteTableColumn($configBucketId . '.projects', 'active');
		$this->configuration->clearCache();
		try {
			$this->configuration->checkProjectsTable();
			$this->fail();
		} catch (WrongConfigurationException $e) {
		}
	}

	public function testProjectsUsersConfiguration()
	{
		$writerId = uniqid();
		$this->configuration->setWriterId($writerId);
		$this->configuration->createBucket($writerId);
		$configBucketId = 'sys.c-wr-gooddata-' . $writerId;

		$pid = uniqid();
		$this->configuration->saveProject($pid);
		$uid = uniqid();
		$email = $uid . '@keboola.com';
		$this->configuration->saveUser($email, $uid);

		// saveProjectUser()
		$this->configuration->saveProjectUser($pid, $email, 'admin', false);
		$this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.project_users'));
		$rawData = $this->storageApiClient->exportTable($configBucketId . '.project_users');
		$data = Client::parseCsv($rawData);
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('pid', $data[0]);
		$this->assertArrayHasKey('email', $data[0]);
		$this->assertArrayHasKey('role', $data[0]);
		$this->assertArrayHasKey('action', $data[0]);
		$this->assertEquals($pid, $data[0]['pid']);
		$this->assertEquals($email, $data[0]['email']);
		$this->assertEquals('admin', $data[0]['role']);
		$this->assertEquals('add', $data[0]['action']);

		// getProjectUsers()
		$data = $this->configuration->getProjectUsers();
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('pid', $data[0]);
		$this->assertArrayHasKey('email', $data[0]);
		$this->assertArrayHasKey('role', $data[0]);
		$this->assertArrayHasKey('action', $data[0]);
		$this->assertEquals($pid, $data[0]['pid']);
		$this->assertEquals($email, $data[0]['email']);
		$this->assertEquals('admin', $data[0]['role']);
		$this->assertEquals('add', $data[0]['action']);
		$data = $this->configuration->getProjectUsers($pid);
		$this->assertCount(1, $data);
		$data = $this->configuration->getProjectUsers(uniqid());
		$this->assertCount(0, $data);

		// isProjectUser()
		$this->assertTrue($this->configuration->isProjectUser($email, $pid));
		$this->assertFalse($this->configuration->isProjectUser($email, uniqid()));

		// deleteProjectUser()
		$this->configuration->deleteProjectUser($pid, $email);
		$data = $this->configuration->getProjectUsers();
		$this->assertCount(0, $data);

		// checkProjectsUsersTable()
		try {
			$this->configuration->checkProjectUsersTable();
		} catch (WrongConfigurationException $e) {
			$this->fail();
		}
		$this->storageApiClient->deleteTableColumn($configBucketId . '.project_users', 'role');
		$this->configuration->clearCache();
		try {
			$this->configuration->checkProjectUsersTable();
			$this->fail();
		} catch (WrongConfigurationException $e) {
		}
	}

	public function testFiltersConfiguration()
	{
		$writerId = uniqid();
		$this->configuration->setWriterId($writerId);
		$this->configuration->createBucket($writerId);
		$configBucketId = 'sys.c-wr-gooddata-' . $writerId;
		$filterName = uniqid();

		// saveFilter()
		$this->configuration->saveFilter($filterName, $this->dataBucketId . 'users.name', '=', 'User 1', 'over1', 'to1');
		$this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.filters'));
		$rawData = $this->storageApiClient->exportTable($configBucketId . '.filters');
		$data = Client::parseCsv($rawData);
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('name', $data[0]);
		$this->assertArrayHasKey('attribute', $data[0]);
		$this->assertArrayHasKey('operator', $data[0]);
		$this->assertArrayHasKey('value', $data[0]);
		$this->assertArrayHasKey('over', $data[0]);
		$this->assertArrayHasKey('to', $data[0]);
		$this->assertEquals($filterName, $data[0]['name']);
		$this->assertEquals($this->dataBucketId . 'users.name', $data[0]['attribute']);
		$this->assertEquals('=', $data[0]['operator']);
		$this->assertEquals('User 1', $data[0]['value']);
		$this->assertEquals('over1', $data[0]['over']);
		$this->assertEquals('to1', $data[0]['to']);

		// getFilters()
		$data = $this->configuration->getFilters();
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('name', $data[0]);
		$this->assertArrayHasKey('attribute', $data[0]);
		$this->assertArrayHasKey('operator', $data[0]);
		$this->assertArrayHasKey('value', $data[0]);
		$this->assertArrayHasKey('over', $data[0]);
		$this->assertArrayHasKey('to', $data[0]);
		$this->assertEquals($filterName, $data[0]['name']);
		$this->assertEquals($this->dataBucketId . 'users.name', $data[0]['attribute']);
		$this->assertEquals('=', $data[0]['operator']);
		$this->assertEquals('User 1', $data[0]['value']);
		$this->assertEquals('over1', $data[0]['over']);
		$this->assertEquals('to1', $data[0]['to']);
		$data = $this->configuration->getFilters(array($filterName));
		$this->assertCount(1, $data);
		$data = $this->configuration->getFilters(array(uniqid()));
		$this->assertCount(0, $data);

		// getFilter()
		$data = $this->configuration->getFilter($filterName);
		$this->assertArrayHasKey('name', $data);
		$this->assertArrayHasKey('attribute', $data);
		$this->assertArrayHasKey('operator', $data);
		$this->assertArrayHasKey('value', $data);
		$this->assertArrayHasKey('over', $data);
		$this->assertArrayHasKey('to', $data);
		$this->assertEquals($filterName, $data['name']);
		$this->assertEquals($this->dataBucketId . 'users.name', $data['attribute']);
		$this->assertEquals('=', $data['operator']);
		$this->assertEquals('User 1', $data['value']);
		$this->assertEquals('over1', $data['over']);
		$this->assertEquals('to1', $data['to']);

		// deleteFilter()
		$this->configuration->deleteFilter($filterName);
		$data = $this->configuration->getFilters();
		$this->assertCount(0, $data);

		// checkFiltersTable()
		try {
			$this->configuration->checkFiltersTable();
		} catch (WrongConfigurationException $e) {
			$this->fail();
		}
		$this->storageApiClient->deleteTableColumn($configBucketId . '.filters', 'operator');
		$this->configuration->clearCache();
		try {
			$this->configuration->checkFiltersTable();
			$this->fail();
		} catch (WrongConfigurationException $e) {
		}
	}

	public function testFiltersProjectsConfiguration()
	{
		$writerId = uniqid();
		$this->configuration->setWriterId($writerId);
		$this->configuration->createBucket($writerId);
		$configBucketId = 'sys.c-wr-gooddata-' . $writerId;

		$pid = uniqid();
		$this->configuration->saveProject($pid);
		$filterName = uniqid();
		$this->configuration->saveFilter($filterName, $this->dataBucketId . 'users.name', '=', 'User 1', 'over1', 'to1');

		// saveFiltersProjects()
		$this->configuration->saveFiltersProjects('uri/' . $filterName, $filterName, $pid);
		$this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.filters_projects'));
		$rawData = $this->storageApiClient->exportTable($configBucketId . '.filters_projects');
		$data = Client::parseCsv($rawData);
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('uri', $data[0]);
		$this->assertArrayHasKey('filter', $data[0]);
		$this->assertArrayHasKey('pid', $data[0]);
		$this->assertEquals('uri/' . $filterName, $data[0]['uri']);
		$this->assertEquals($filterName, $data[0]['filter']);
		$this->assertEquals($pid, $data[0]['pid']);

		// getFiltersProjects()
		$data = $this->configuration->getFiltersProjects();
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('uri', $data[0]);
		$this->assertArrayHasKey('filter', $data[0]);
		$this->assertArrayHasKey('pid', $data[0]);
		$this->assertEquals('uri/' . $filterName, $data[0]['uri']);
		$this->assertEquals($filterName, $data[0]['filter']);
		$this->assertEquals($pid, $data[0]['pid']);

		// getFiltersProjectsByFilter()
		$data = $this->configuration->getFiltersProjectsByFilter($filterName);
		$this->assertCount(1, $data);
		$data = $this->configuration->getFiltersProjectsByFilter(uniqid());
		$this->assertCount(0, $data);

		// getFiltersProjectsByPid()
		$data = $this->configuration->getFiltersProjectsByPid($pid);
		$this->assertCount(1, $data);
		$data = $this->configuration->getFiltersProjectsByPid(uniqid());
		$this->assertCount(0, $data);

		// getFiltersForProject()
		$data = $this->configuration->getFiltersForProject($pid);
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('name', $data[0]);
		$this->assertArrayHasKey('attribute', $data[0]);
		$this->assertArrayHasKey('operator', $data[0]);
		$this->assertArrayHasKey('value', $data[0]);
		$this->assertArrayHasKey('over', $data[0]);
		$this->assertArrayHasKey('to', $data[0]);
		$this->assertEquals($filterName, $data[0]['name']);
		$this->assertEquals($this->dataBucketId . 'users.name', $data[0]['attribute']);
		$this->assertEquals('=', $data[0]['operator']);
		$data = $this->configuration->getFiltersForProject(uniqid());
		$this->assertCount(0, $data);

		// deleteFilterFromProject()
		$this->configuration->deleteFilterFromProject('uri/' . $filterName);
		$data = $this->configuration->getFiltersProjects();
		$this->assertCount(0, $data);
	}

	public function testFiltersUsersConfiguration()
	{
		$writerId = uniqid();
		$this->configuration->setWriterId($writerId);
		$this->configuration->createBucket($writerId);
		$configBucketId = 'sys.c-wr-gooddata-' . $writerId;

		$uid = uniqid();
		$email = $uid . '@keboola.com';
		$this->configuration->saveUser($email, $uid);
		$filterName = uniqid();
		$this->configuration->saveFilter($filterName, $this->dataBucketId . 'users.name', '=', 'User 1', 'over1', 'to1');

		// saveFiltersUsers()
		$this->configuration->saveFiltersUsers(array($filterName), $email);
		$this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.filters_users'));
		$rawData = $this->storageApiClient->exportTable($configBucketId . '.filters_users');
		$data = Client::parseCsv($rawData);
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('id', $data[0]);
		$this->assertArrayHasKey('filter', $data[0]);
		$this->assertArrayHasKey('email', $data[0]);
		$this->assertEquals($filterName, $data[0]['filter']);
		$this->assertEquals($email, $data[0]['email']);

		// getFiltersUsers()
		$data = $this->configuration->getFiltersUsers();
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('id', $data[0]);
		$this->assertArrayHasKey('filter', $data[0]);
		$this->assertArrayHasKey('email', $data[0]);
		$this->assertEquals($filterName, $data[0]['filter']);
		$this->assertEquals($email, $data[0]['email']);

		// getFiltersUsersByEmail()
		$data = $this->configuration->getFiltersUsersByEmail($email);
		$this->assertCount(1, $data);
		$data = $this->configuration->getFiltersUsersByEmail(uniqid());
		$this->assertCount(0, $data);

		// getFiltersUsersByFilter()
		$data = $this->configuration->getFiltersUsersByFilter($filterName);
		$this->assertCount(1, $data);
		$data = $this->configuration->getFiltersUsersByFilter(uniqid());
		$this->assertCount(0, $data);

		// getFiltersForUser()
		$data = $this->configuration->getFiltersForUser($email);
		$this->assertCount(1, $data);
		$this->assertArrayHasKey('name', $data[0]);
		$this->assertArrayHasKey('attribute', $data[0]);
		$this->assertArrayHasKey('operator', $data[0]);
		$this->assertArrayHasKey('value', $data[0]);
		$this->assertArrayHasKey('over', $data[0]);
		$this->assertArrayHasKey('to', $data[0]);
		$this->assertEquals($filterName, $data[0]['name']);
		$this->assertEquals($this->dataBucketId . 'users.name', $data[0]['attribute']);
		$this->assertEquals('=', $data[0]['operator']);
		$data = $this->configuration->getFiltersForProject(uniqid());
		$this->assertCount(0, $data);
	}

	

}