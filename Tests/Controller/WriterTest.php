<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Client;
use Symfony\Component\Console\Tester\CommandTester;
use Keboola\GoodDataWriter\Command\RunJobCommand,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

class WriterTest extends WebTestCase
{
	const BUCKET_NAME = 'wr-gooddata-test';
	const BUCKET_ID = 'sys.c-wr-gooddata-test';
	const DATA_BUCKET_ID = 'out.c-gdwtest';
	const WRITER_ID = 'test';

	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected static $storageApi;
	/**
	 * @var \Keboola\GoodDataWriter\GoodData\RestApi
	 */
	protected static $restApi;
	/**
	 * @var \Symfony\Bundle\FrameworkBundle\Client
	 */
	protected static $client;
	/**
	 * @var CommandTester
	 */
	protected static $commandTester;
	protected static $mainConfig;

	public static function setUpBeforeClass()
	{
		self::$client = static::createClient();
		$container = self::$client->getContainer();
		self::$client->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => $container->getParameter('storageApi.test.token')
		));

		self::$mainConfig = $container->getParameter('gooddata_writer');
		self::$storageApi = new \Keboola\StorageApi\Client($container->getParameter('storageApi.test.token'),
			self::$client->getContainer()->getParameter('storageApi.url'));
		self::$restApi = new \Keboola\GoodDataWriter\GoodData\RestApi(null, $container->get('logger'));

		// Clear test environment
		if (self::$storageApi->bucketExists(self::BUCKET_ID)) {
			$bucketInfo = self::$storageApi->getBucket(self::BUCKET_ID);
			foreach ($bucketInfo['tables'] as $table) {
				self::$storageApi->dropTable($table['id']);
			}
			self::$storageApi->dropBucket(self::BUCKET_ID);
		}
		if (self::$storageApi->bucketExists(self::DATA_BUCKET_ID)) {
			$bucketInfo = self::$storageApi->getBucket(self::DATA_BUCKET_ID);
			foreach ($bucketInfo['tables'] as $table) {
				self::$storageApi->dropTable($table['id']);
			}
			self::$storageApi->dropBucket(self::DATA_BUCKET_ID);
		}

		// Init job processing
		$application = new Application(self::$client->getKernel());
		$application->add(new RunJobCommand());
		$command = $application->find('gooddata-writer:run-job');
		self::$commandTester = new CommandTester($command);
	}

	public static function tearDownAfterClass()
	{

	}

	public function testCreateWriter()
	{
		// Create and process job
		$this->_processJob('/gooddata-writer/writers', array());

		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		// Check result
		$validConfiguration = true;
		try {
			$configuration->checkGoodDataSetup();
		} catch (\Keboola\GoodDataWriter\Exception\WrongConfigurationException $e) {
			$validConfiguration = false;
		}
		$this->assertTrue($validConfiguration);

		self::$restApi->login($configuration->bucketInfo['gd']['username'], $configuration->bucketInfo['gd']['password']);
		$projectInfo = self::$restApi->getProject($configuration->bucketInfo['gd']['pid']);
		$this->assertArrayHasKey('project', $projectInfo);
		$this->assertArrayHasKey('content', $projectInfo['project']);
		$this->assertArrayHasKey('state', $projectInfo['project']['content']);
		$this->assertEquals('ENABLED', $projectInfo['project']['content']['state']);

		$userInfo = self::$restApi->getUser($configuration->bucketInfo['gd']['uid']);
		$this->assertArrayHasKey('accountSetting', $userInfo);

		$userProjectsInfo = self::$restApi->get(sprintf('/gdc/account/profile/%s/projects', $configuration->bucketInfo['gd']['uid']));
		$this->assertArrayHasKey('projects', $userProjectsInfo);
		$this->assertCount(1, $userProjectsInfo['projects']);
		$projectFound = false;
		foreach ($userProjectsInfo['projects'] as $p) {
			if (isset($p['project']['links']['metadata']) && strpos($p['project']['links']['metadata'], $configuration->bucketInfo['gd']['pid']) !== false) {
				$projectFound = true;
				break;
			}
		}
		$this->assertTrue($projectFound);
	}

	public function testUploadProject()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		// Prepare data
		self::$storageApi->createBucket('gdwtest', 'out', 'Writer Test');

		$table = new StorageApiTable(self::$storageApi, self::DATA_BUCKET_ID . '.categories', null, 'id');
		$table->setHeader(array('id', 'name'));
		$table->setFromArray(array(
			array('c1', 'Category 1'),
			array('c2', 'Category 2')
		));
		$table->save();

		$table = new StorageApiTable(self::$storageApi, self::DATA_BUCKET_ID . '.products', null, 'id');
		$table->setHeader(array('id', 'name', 'price', 'date', 'category'));
		$table->setFromArray(array(
			array('p1', 'Product 1', '45', '2013-01-01', 'c1'),
			array('p2', 'Product 2', '26', '2013-01-03', 'c2'),
			array('p3', 'Product 3', '112', '2013-01-03', 'c1')
		));
		$table->save();

		// Prepare Writer configuration
		$configuration->addDateDimension('ProductDate', false);

		$table = new StorageApiTable(self::$storageApi, 'sys.c-wr-gooddata-test.c-gdwtest_categories', null, 'name');
		$table->setAttribute('tableId', self::DATA_BUCKET_ID . '.categories');
		$table->setAttribute('gdName', 'Categories');
		$table->setAttribute('export', '1');
		$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
			'format', 'dateDimension', 'sortLabel', 'sortOrder'));
		$table->setFromArray(array(
			array('id', 'Id', 'CONNECTION_POINT', '', '', '', '', '', '', '', ''),
			array('name', 'Name', 'ATTRIBUTE', '', '', '', '', '', '', '', '')
		));
		$table->save();

		$table = new StorageApiTable(self::$storageApi, 'sys.c-wr-gooddata-test.c-gdwtest_products', null, 'name');
		$table->setAttribute('tableId', self::DATA_BUCKET_ID . '.products');
		$table->setAttribute('gdName', 'Products');
		$table->setAttribute('export', '1');
		$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
			'format', 'dateDimension', 'sortLabel', 'sortOrder'));
		$table->setFromArray(array(
			array('id', 'Id', 'CONNECTION_POINT', '', '', '', '', '', '', '', ''),
			array('name', 'Name', 'ATTRIBUTE', '', '', '', '', '', '', '', ''),
			array('price', 'Price', 'FACT', '', '', '', '', '', '', '', ''),
			array('date', 'Date', 'DATE', '', '', '', '', 'yyyy-MM-dd', 'ProductDate', '', ''),
			array('category', 'Category', 'REFERENCE', '', '', self::DATA_BUCKET_ID . '.categories', '', '', '', '', '')
		));
		$table->save();

		// Create and process job
		$this->_processJob('/gooddata-writer/upload-project', array());


		$datasetsData = self::$restApi->get('/gdc/md/' . $configuration->bucketInfo['gd']['pid'] . '/data/sets');
		$this->assertArrayHasKey('dataSetsInfo', $datasetsData);
		$this->assertArrayHasKey('sets', $datasetsData['dataSetsInfo']);
		$this->assertCount(3, $datasetsData['dataSetsInfo']['sets']);

	}

	public function testGetModel()
	{
		//@TODO exit() in GoodDataWriter::getModel() stops test execution
		/*self::$client->request('GET', sprintf('/gooddata-writer/model?writerId=%s', self::WRITER_ID));
		$response = self::$client->getResponse();
		$responseJson = json_decode($response->getContent(), true);

		$this->assertArrayHasKey('nodes', $responseJson);
		$this->assertArrayHasKey('links', $responseJson);
		$this->assertCount(2, $responseJson['nodes']);
		$this->assertCount(1, $responseJson['links']);*/
	}

	public function testGetTables()
	{
		self::$client->request('GET', '/gooddata-writer/tables?writerId=' . self::WRITER_ID);
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertArrayHasKey('tables', $responseJson);

		// Filter out tables not belonging to this test
		$tables = array();
		foreach ($responseJson['tables'] as $t) {
			if ($t['bucket'] == self::DATA_BUCKET_ID) {
				$tables[] = $t;
			}
		}

		$this->assertCount(2, $tables);
		foreach ($tables as $table) {
			$this->assertArrayHasKey('gdName', $table);
			$this->assertTrue(in_array($table['gdName'], array('Products', 'Categories')));
			$this->assertArrayHasKey('lastExportDate', $table);
		}

	}

	public function testPostTables()
	{
		$tableId = self::DATA_BUCKET_ID . '.products';
		$testName = uniqid('test-name');

		self::$client->request('POST', '/gooddata-writer/tables', array(), array(), array(),
			json_encode(array(
				'writerId' => self::WRITER_ID,
				'tableId' => $tableId,
				'gdName' => $testName
			)));
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);


		self::$client->request('GET', '/gooddata-writer/tables?writerId=' . self::WRITER_ID);
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertArrayHasKey('tables', $responseJson);

		$testResult = false;
		$lastChangeDate = null;
		foreach ($responseJson['tables'] as $t) {
			if ($t['id'] == $tableId) {
				$this->assertArrayHasKey('gdName', $t);
				if ($t['gdName'] == $testName) {
					$testResult = true;
				}
				$lastChangeDate = $t['lastChangeDate'];
			}
		}
		$this->assertTrue($testResult);
		$this->assertNotEmpty($lastChangeDate);

		self::$client->request('POST', '/gooddata-writer/tables', array(), array(), array(),
			json_encode(array(
				'writerId' => self::WRITER_ID,
				'tableId' => $tableId,
				'gdName' => $testName . '2'
			)));
		self::$client->request('GET', '/gooddata-writer/tables?writerId=' . self::WRITER_ID);
		$response = self::$client->getResponse();
		$responseJson = json_decode($response->getContent(), true);

		$lastChangeDateAfterUpdate = null;
		foreach ($responseJson['tables'] as $t) {
			if ($t['id'] != $tableId) {
				continue;
			}
			$lastChangeDateAfterUpdate = $t['lastChangeDate'];
		}

		$this->assertNotEquals($lastChangeDate, $lastChangeDateAfterUpdate, 'Last change date should be changed after update');
	}



	public function testUploadTable()
	{
		//@TODO need to test that load data finished successfully
	}

	public function testGetWriters()
	{
		self::$client->request('GET', '/gooddata-writer/writers');
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('writers', $responseJson);
		$this->assertCount(1, $responseJson['writers']);
		$this->assertEquals(self::WRITER_ID, $responseJson['writers'][0]['id']);
	}

	public function testCreateProject()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		// Create and process job
		$this->_processJob('/gooddata-writer/projects', array());

		// Check result
		$projectsList = $configuration->getProjects();
		$this->assertCount(2, $projectsList);

		$project = $projectsList[1];
		self::$restApi->login($configuration->bucketInfo['gd']['username'], $configuration->bucketInfo['gd']['password']);
		$projectInfo = self::$restApi->getProject($project['pid']);
		$this->assertArrayHasKey('project', $projectInfo);
		$this->assertArrayHasKey('content', $projectInfo['project']);
		$this->assertArrayHasKey('state', $projectInfo['project']['content']);
		$this->assertEquals('ENABLED', $projectInfo['project']['content']['state']);
	}

	public function testGetProjects()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		$projectsList = $configuration->getProjects();
		$project = $projectsList[1];

		self::$client->request('GET', '/gooddata-writer/projects?writerId=' . self::WRITER_ID);
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('projects', $responseJson);
		$this->assertCount(2, $responseJson['projects']);
		$this->assertEquals($project['pid'], $responseJson['projects'][1]['pid']);
	}

	public function testCreateUser()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		// Create and process job
		$this->_processJob('/gooddata-writer/users', array(
			'email' => 'test' . time() . uniqid() . '@test.keboola.com',
			'password' => md5(uniqid()),
			'firstName' => 'Test',
			'lastName' => 'KBC'
		));

		// Check result
		$usersList = $configuration->getUsers();
		$this->assertCount(2, $usersList);

		$user = $usersList[1];
		$userInfo = self::$restApi->getUser($user['uid']);
		$this->assertArrayHasKey('accountSetting', $userInfo);
	}

	public function testGetUsers()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		self::$client->request('GET', '/gooddata-writer/users?writerId=' . self::WRITER_ID);
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('users', $responseJson);
		$this->assertCount(2, $responseJson['users']);
		$this->assertEquals($user['uid'], $responseJson['users'][1]['uid']);
	}

	public function testAddUserToProject()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		$projectsList = $configuration->getProjects();
		$project = $projectsList[1];

		// Create and process job
		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check result
		$userProjectsInfo = self::$restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo);
		$this->assertCount(3, $userProjectsInfo['users']);
		$userInProject = false;
		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject);
	}

	public function testInviteUserToProject()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		$projectsList = $configuration->getProjects();
		$project = $projectsList[0];

		// Create and process job
		$this->_processJob('/gooddata-writer/project-invitations', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check result
		$userProjectsInfo = self::$restApi->get('/gdc/projects/' . $project['pid'] . '/invitations');
		$this->assertArrayHasKey('invitations', $userProjectsInfo);
		$this->assertCount(1, $userProjectsInfo['invitations']);
		$userInProject = false;
		foreach ($userProjectsInfo['invitations'] as $p) {
			if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject);
	}

	public function testGetProjectUsers()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		$projectsList = $configuration->getProjects();
		$project = $projectsList[1];

		self::$client->request('GET', '/gooddata-writer/project-users?writerId=' . self::WRITER_ID . '&pid=' . $project['pid']);
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('users', $responseJson);
		$this->assertCount(1, $responseJson['users']);
		$this->assertEquals($user['email'], $responseJson['users'][0]['email']);
	}

	public function testCreateFilter()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		// Create and process job
		$this->_processJob('/gooddata-writer/filters', array(
			"pid"       => $configuration->bucketInfo['gd']['pid'],
			"name"      => "filter",
			"attribute" => "Name (Products)",
			"element"   => "Product 1"
		));

		// Check result
		$filterList = $configuration->getFilters();
		$this->assertCount(1, $filterList);
	}

	public function testAssignFilterToUser()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		$usersList = $configuration->getUsers();
		$user = $usersList[0];

		$filters = $configuration->getFilters();
		$filter = $filters[0];

		// Create and process job
		$this->_processJob('/gooddata-writer/filters-user', array(
			"pid"       => $configuration->bucketInfo['gd']['pid'],
			"filters"   => array($filter['name']),
			"userEmail"    => $user['email']
		));

		// Check result
		$filtersUsers = $configuration->getFiltersUsers();
		$this->assertCount(1, $filtersUsers);
	}

	public function testSyncFilter()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		$pid = $configuration->bucketInfo['gd']['pid'];

		// Create and process job
		$this->_processJob('/gooddata-writer/sync-filters', array(
			"pid"   => $pid,
		));

		// Check result
		$filterList = $configuration->getFilters();

		$gdFilters = self::$restApi->getFilters($pid);
		$gdFilter = $gdFilters[0];

		$this->assertEquals($gdFilter['link'], $filterList[0]['uri']);
	}

	public function testDeleteFilter()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		$filters = $configuration->getFilters();
		$filter = $filters[0];

		// Create and process job
		$this->_processJob(
			'/gooddata-writer/filters?writerId=' . self::WRITER_ID . '&uri=' . $filter['uri'] . '&dev=1',
			array(),
			'DELETE'
		);

		// Check result
		$filters = $configuration->getFilters();
		$this->assertCount(0, $filters);

		$filtersUsers = $configuration->getFiltersUsers();
		$this->assertCount(0, $filtersUsers);
	}

	public function testCancelJobs()
	{
		self::$client->request('POST', '/gooddata-writer/upload-project', array(), array(), array(),
			json_encode(array(
				'writerId' => self::WRITER_ID,
				'dev' => 1
			)));
		$response = self::$client->getResponse();

		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('batch', $responseJson);


		self::$client->request('GET', sprintf('/gooddata-writer/batch?writerId=%s&id=%d', self::WRITER_ID, $responseJson['batch']));
		$response = self::$client->getResponse();
		$responseJson = json_decode($response->getContent(), true);
		$this->assertArrayHasKey('batch', $responseJson);
		$this->assertArrayHasKey('jobs', $responseJson['batch']);
		$jobs = $responseJson['batch']['jobs'];


		self::$client->request('POST', '/gooddata-writer/cancel-jobs', array(), array(), array(),
			json_encode(array(
				'writerId' => self::WRITER_ID,
				'dev' => 1
			)));
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);


		foreach ($jobs as $jobId) {
			self::$client->request('GET', sprintf('/gooddata-writer/job?writerId=%s&id=%d', self::WRITER_ID, $jobId));
			$response = self::$client->getResponse();
			$responseJson = json_decode($response->getContent(), true);
			$this->assertArrayHasKey('status', $responseJson);
			$this->assertArrayHasKey('job', $responseJson);
			$this->assertArrayHasKey('status', $responseJson['job']);
			$this->assertEquals('cancelled', $responseJson['job']['status']);
		}
	}

	public function testDeleteWriter()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			self::$mainConfig['tmp_path']);

		self::$restApi->login(self::$mainConfig['gd']['dev']['username'], self::$mainConfig['gd']['dev']['password']);

		foreach ($configuration->getProjects() as $p) {
			self::$restApi->dropProject($p['pid']);
		}
		foreach ($configuration->getUsers() as $u) {
			self::$restApi->dropUser($u['uid']);
		}

		// Create and process job
		$this->_processJob('/gooddata-writer/delete-writers', array());

		// Check result
		$this->assertFalse($configuration->configurationBucket(self::WRITER_ID));
	}


	/**
	 * Call API and process the job immediately
	 * @param $url
	 * @param $params
	 * @param string $method
	 */
	protected function _processJob($url, $params, $method = 'POST')
	{
		self::$client->request($method, $url, array(), array(), array(),
			json_encode(array_merge($params, array(
				'writerId' => self::WRITER_ID,
				'dev' => 1
			))));
		$response = self::$client->getResponse();

		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);

		if (isset($responseJson['job'])) {
			self::$commandTester->execute(array(
				'command' => 'gooddata-writer:run-job',
				'job' => $responseJson['job']
			));
		} else if (isset($responseJson['batch'])) {
			self::$client->request('GET', sprintf('/gooddata-writer/batch?writerId=%s&id=%d', self::WRITER_ID, $responseJson['batch']));
			$response = self::$client->getResponse();
			$responseJson = json_decode($response->getContent(), true);
			$this->assertArrayHasKey('batch', $responseJson);
			$this->assertArrayHasKey('jobs', $responseJson['batch']);
			foreach ($responseJson['batch']['jobs'] as $job) {
				self::$commandTester->execute(array(
					'command' => 'gooddata-writer:run-job',
					'job' => $job
				));
			}
		} else {
			$this->assertTrue(false);
		}
	}

}
