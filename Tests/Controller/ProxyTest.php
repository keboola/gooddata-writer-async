<?php
/**
 * @author Erik Zigo <erik.zigo@keboola.com>
 * @date 2013-10-24
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Writer\SharedConfig;

class ProxyTest extends AbstractControllerTest
{
	public function testProxy()
	{
		$user = $this->createUser();

		/**
		 * Get proxy
		 */
		
		// Check of Writer API
		$projectsList = $this->configuration->getProjects();
		$this->assertGreaterThanOrEqual(1, $projectsList, "Response for writer call '/projects' should return at least one GoodData project.");
		$project = $projectsList[count($projectsList)-1];

		$url = sprintf('/gdc/projects/%s/users?offset=0&limit=2', $project['pid']);

		$responseJson = $this->getWriterApi('/proxy?writerId=' . $this->writerId . '&query=' . urlencode($url));

		$this->assertArrayHasKey('response', $responseJson, "Response for writer call '/proxy' should contain 'response' key.");
		$this->assertArrayHasKey('users', $responseJson['response'], "Response for writer call '/proxy' should contain 'users' key users in GD reponse.");
		$this->assertCount(2, $responseJson['response']['users'], "Response for writer call '/proxy' should return two users in GD reponse.");


		/**
		 * Post proxy
		 */
		// Upload data
		$this->prepareData();
		$this->processJob('/upload-project');

		$bucketAttributes = $this->configuration->bucketAttributes();
		$pid = $bucketAttributes['gd']['pid'];

		$attr = $this->getAttributeByTitle($pid, 'Id (Categories)');

		$attrUri = $attr['attribute']['meta']['uri'];

		// repost attribute to GD
		$jobId = $this->processJob('/proxy', array(
			'writerId'  => $this->writerId,
			'query'     => $attrUri,
			'payload'   => $attr
		), 'POST');

		$jobStatus = $this->getWriterApi('/jobs?jobId=' .$jobId . '&writerId=' . $this->writerId);

		$this->assertEquals(SharedConfig::JOB_STATUS_SUCCESS, $jobStatus['status']);
	}
}
