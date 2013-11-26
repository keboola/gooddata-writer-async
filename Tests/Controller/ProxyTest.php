<?php
/**
 * @author Erik Zigo <erik.zigo@keboola.com>
 * @date 2013-10-24
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

class ProxyTest extends AbstractControllerTest
{
	public function testGetProxy()
	{
		$user = $this->_createUser();

		// Check of GoodData
		$bucketInfo = self::$configuration->bucketInfo();
		self::$restApi->setCredentials($bucketInfo['gd']['username'], $bucketInfo['gd']['password']);
		$userInfo = self::$restApi->getUser($user['uid']);
		$this->assertArrayHasKey('accountSetting', $userInfo, "Response for GoodData API user call should contain 'accountSetting' key.");

		// Check of Writer API
		$projectsList = self::$configuration->getProjects();
		$this->assertGreaterThanOrEqual(1, $projectsList, "Response for writer call '/projects' should return at least one GoodData project.");
		$project = $projectsList[count($projectsList)-1];

		$url = sprintf('/gdc/projects/%s/users?offset=0&limit=2', $project['pid']);

		$responseJson = $this->_getWriterApi('/gooddata-writer/proxy?writerId=' . $this->writerId . '&query=' . urlencode($url));

		$this->assertArrayHasKey('response', $responseJson, "Response for writer call '/proxy' should contain 'response' key.");
		$this->assertArrayHasKey('users', $responseJson['response'], "Response for writer call '/proxy' should contain 'users' key users in GD reponse.");
		$this->assertCount(2, $responseJson['response']['users'], "Response for writer call '/proxy' should return two users in GD reponse.");
	}
}
