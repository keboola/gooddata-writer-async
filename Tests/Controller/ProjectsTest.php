<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

class ProjectsTest extends AbstractControllerTest
{


	public function testCreateProject()
	{
		$this->_processJob('/gooddata-writer/projects', array());

		// Check of configuration
		$projectsList = self::$configuration->getProjects();
		$this->assertCount(2, $projectsList, "Response for writer call '/projects' should return two GoodData projects.");
		$project = $projectsList[1];


		// Check of GoodData
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$projectInfo = self::$restApi->getProject($project['pid']);
		$this->assertArrayHasKey('project', $projectInfo, "Response for GoodData API project call should contain 'project' key.");
		$this->assertArrayHasKey('content', $projectInfo['project'], "Response for GoodData API project call should contain 'project.content' key.");
		$this->assertArrayHasKey('state', $projectInfo['project']['content'], "Response for GoodData API project call should contain 'project.content.state' key.");
		$this->assertEquals('ENABLED', $projectInfo['project']['content']['state'], "Response for GoodData API project call should contain 'project.content.state' key with value 'ENABLED'.");


		// Check of Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/projects?writerId=' . $this->writerId);
		$this->assertArrayHasKey('projects', $responseJson, "Response for writer call '/projects' should contain 'projects' key.");
		$this->assertCount(2, $responseJson['projects'], "Response for writer call '/projects' should return two projects.");
		$projectFound = false;
		foreach ($responseJson['projects'] as $p) {
			if ($p['pid'] == $project['pid']) {
				$projectFound = true;
			}
		}
		$this->assertTrue($projectFound, "Response for writer call '/projects' should return tested project.");
	}


}
