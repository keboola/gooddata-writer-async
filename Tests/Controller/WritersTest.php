<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Test\WriterTest,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

class WritersTest extends WriterTest
{

	public function testCreateWriter()
	{
		// Check writer configuration
		$validConfiguration = true;
		try {
			self::$configuration->checkGoodDataSetup();
		} catch (WrongConfigurationException $e) {
			$validConfiguration = false;
		}
		$this->assertTrue($validConfiguration, "Writer configuration is not valid.");

		// Check project existence in GD
		self::$restApi->login(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$projectInfo = self::$restApi->getProject(self::$configuration->bucketInfo['gd']['pid']);
		$this->assertArrayHasKey('project', $projectInfo, "Response for GoodData API login call should contain 'project' key.");
		$this->assertArrayHasKey('content', $projectInfo['project'], "Response for GoodData API login call should contain 'project.content' key.");
		$this->assertArrayHasKey('state', $projectInfo['project']['content'], "Response for GoodData API login call should contain 'project.content.state' key.");
		$this->assertEquals('ENABLED', $projectInfo['project']['content']['state'], "Response for GoodData API login call should have key 'project.content.state' with value 'ENABLED'.");

		// Check user existence in GD
		$userInfo = self::$restApi->getUser(self::$configuration->bucketInfo['gd']['uid']);
		$this->assertArrayHasKey('accountSetting', $userInfo, "Response for GoodData API user info call should contain 'accountSetting' key.");

		// Check user's access to the project in GD
		$userProjectsInfo = self::$restApi->get(sprintf('/gdc/account/profile/%s/projects', self::$configuration->bucketInfo['gd']['uid']));
		$this->assertArrayHasKey('projects', $userProjectsInfo, "Response for GoodData API user projects call should contain 'projects' key.");
		$this->assertCount(1, $userProjectsInfo['projects'], "Writer's primary user should have exactly one project assigned.");
		$projectFound = false;
		foreach ($userProjectsInfo['projects'] as $p) {
			if (isset($p['project']['links']['metadata']) && strpos($p['project']['links']['metadata'], self::$configuration->bucketInfo['gd']['pid']) !== false) {
				$projectFound = true;
				break;
			}
		}
		$this->assertTrue($projectFound, "Writer's primary user should have assigned master project.");
	}


	public function testDeleteWriter()
	{
		self::$restApi->login(self::$mainConfig['gd']['dev']['username'], self::$mainConfig['gd']['dev']['password']);

		$this->_processJob('/gooddata-writer/delete-writers');

		// Check non-existence of configuration
		$this->assertFalse(self::$configuration->configurationBucket($this->writerId), "Writer configuration should not exist anymore.");
	}


	public function testGetWriters()
	{
		$responseJson = $this->_getWriterApi('/gooddata-writer/writers');

		$this->assertArrayHasKey('writers', $responseJson, "Response for writer call '/writers' should contain 'writers' key.");
		$this->assertCount(1, $responseJson['writers'], "Response for writer call '/writers' should contain only one writer.");
		$this->assertArrayHasKey('id', $responseJson['writers'][0], "Response for writer call '/writers' should contain 'writers..id' key.");
		$this->assertArrayHasKey('bucket', $responseJson['writers'][0], "Response for writer call '/writers' should contain 'writers..bucket' key.");
		$this->assertEquals($this->writerId, $responseJson['writers'][0]['id'], "Response for writer call '/writers' should contain id of created writer.");
		$this->assertEquals($this->bucketId, $responseJson['writers'][0]['bucket'], "Response for writer call '/writers' should contain bucket of created writer.");


		$responseJson = $this->_getWriterApi('/gooddata-writer/writers?writerId=' . $this->writerId);

		$this->assertArrayHasKey('writer', $responseJson, "Response for writer call '/writers?writerId=' should contain 'writer' key.");
		$this->assertArrayHasKey('writer', $responseJson['writer'], "Response for writer call '/writers?writerId=' should contain 'writer.writer' key.");
		$this->assertArrayHasKey('writerId', $responseJson['writer'], "Response for writer call '/writers?writerId=' should contain 'writer.writerId' key.");
		$this->assertEquals($this->writerId, $responseJson['writer']['writerId'], "Response for writer call '/writers?writerId=' should contain id of created writer.");
		$this->assertEquals('gooddata', $responseJson['writer']['writer'], "Response for writer call '/writers?writerId=' should contain name of the writer.");
	}

}
