<?php
/**
 * @author Erik Zigo <erik.zigo@keboola.com>
 * @date 2013-10-24
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

class ProxyTest extends AbstractControllerTest
{
	public function testProxy()
	{
		$user = $this->_createUser();

		/**
		 * Get proxy
		 */
		// Check of GoodData
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$userInfo = $this->restApi->getUser($user['uid']);
		$this->assertArrayHasKey('accountSetting', $userInfo, "Response for GoodData API user call should contain 'accountSetting' key.");

		// Check of Writer API
		$projectsList = $this->configuration->getProjects();
		$this->assertGreaterThanOrEqual(1, $projectsList, "Response for writer call '/projects' should return at least one GoodData project.");
		$project = $projectsList[count($projectsList)-1];

		$url = sprintf('/gdc/projects/%s/users?offset=0&limit=2', $project['pid']);

		$responseJson = $this->_getWriterApi('/gooddata-writer/proxy?writerId=' . $this->writerId . '&query=' . urlencode($url));

		$this->assertArrayHasKey('response', $responseJson, "Response for writer call '/proxy' should contain 'response' key.");
		$this->assertArrayHasKey('users', $responseJson['response'], "Response for writer call '/proxy' should contain 'users' key users in GD reponse.");
		$this->assertCount(2, $responseJson['response']['users'], "Response for writer call '/proxy' should return two users in GD reponse.");


		/**
		 * Post proxy
		 */
		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$bucketAttributes = $this->configuration->bucketAttributes();
		$pid = $bucketAttributes['gd']['pid'];

		$attr = $this->getAttributeByTitle($pid, 'Id (Categories)');

		$attrUri = $attr['attribute']['meta']['uri'];

		// repost attribute to GD
		$jobId = $this->_processJob('/gooddata-writer/proxy', array(
			'writerId'  => $this->writerId,
			'query'     => $attrUri,
			'payload'   => $attr
		), 'POST');

		$jobStatus = $this->_getWriterApi('/gooddata-writer/jobs?jobId=' .$jobId . '&writerId=' . $this->writerId);

		$this->assertEquals('success', $jobStatus['job']['result']['status']);
	}

	protected function getAttributes($pid)
	{
		$query = sprintf('/gdc/md/%s/query/attributes', $pid);

		$result = $this->_getWriterApi('/gooddata-writer/proxy?writerId=' . $this->writerId . '&query=' . $query);

		if (isset($result['response']['query']['entries'])) {
			return $result['response']['query']['entries'];
		} else {
			throw new \Exception('Attributes in project could not be fetched');
		}
	}

	public function getAttributeByTitle($pid, $title)
	{
		foreach ($this->getAttributes($pid) as $attr) {
			if ($attr['title'] == $title) {
				$result = $this->_getWriterApi('/gooddata-writer/proxy?writerId=' . $this->writerId . '&query=' . $attr['link']);
				return $result['response'];
			}
		}
		return false;
	}
}
