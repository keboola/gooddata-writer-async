<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

class ListProjectsTest extends WebTestCase
{
	const BUCKET_NAME = 'wr-gooddata-test';
	const BUCKET_ID = 'sys.c-wr-gooddata-test';

	/**
	 * @var \Keboola\StorageApi\Client
	 */
	private $_storageApi;
	/**
	 * @var \Keboola\GoodDataWriterBundle\GoodData\RestApi
	 */
	private $_restApi;

	public function _init()
	{
		$client = static::createClient();
		$this->_storageApi = new \Keboola\StorageApi\Client($client->getContainer()->getParameter('storageApi.test.token'),
			$client->getContainer()->getParameter('storageApi.url'));

		if ($this->_storageApi->bucketExists(self::BUCKET_ID)) {
			$bucketInfo = $this->_storageApi->getBucket(self::BUCKET_ID);
			foreach ($bucketInfo['tables'] as $table) {
				$this->_storageApi->dropTable($table['id']);
			}
			$this->_storageApi->dropBucket(self::BUCKET_ID);
		}
		$this->_storageApi->createBucket(self::BUCKET_NAME, 'sys', 'GoodData Writer Test');

		$this->_restApi = new \Keboola\GoodDataWriterBundle\GoodData\RestApi(null, $client->getContainer()->get('log'));
		$pid = $this->_restApi->createProject('Project for testing', $client->getContainer()->getParameter('gd.access_token'));

		$table = new \Keboola\StorageApi\Table($this->_storageApi, self::BUCKET_ID . '.projects');
		$table->setHeader(array('pid', 'active'));
		$table->setFromArray(array(array($pid, 1)));
		$table->save();

		$table = new \Keboola\StorageApi\Table($this->_storageApi, self::BUCKET_ID . '.users');
		$table->setHeader(array('email', 'uri'));
		$table->save();

		$table = new \Keboola\StorageApi\Table($this->_storageApi, self::BUCKET_ID . '.project_users');
		$table->setHeader(array('id', 'pid', 'email', 'role'));
		$table->save();

	}

	public function _cleanup()
	{
		$bucketInfo = $this->_storageApi->getBucket(self::BUCKET_ID);
		foreach ($bucketInfo['tables'] as $table) {
			$this->_storageApi->dropTable($table['id']);
		}
		$this->_storageApi->dropBucket(self::BUCKET_ID);

		//@TODO drop project $this->_restApi->
	}

	public function testListProjects()
	{
		//$this->_init();

		$client = static::createClient();
		$client->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => $client->getContainer()->getParameter('storageApi.test.token'),
			'HTTP_X-StorageApi-Url' => $client->getContainer()->getParameter('storageApi.test.url')
		));
		$crawler = $client->request('GET', '/wr-gooddata/projects', array('writerId' => 'xxx'));

		$response = $client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
	}

}
