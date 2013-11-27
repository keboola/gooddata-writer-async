<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 22/11/13
 * Time: 15:05
 */

namespace Keboola\GoodDataWriter\Tests\Controller;


use Exception;

class ObjectsTest extends AbstractControllerTest
{
	public function testPostObject()
	{
		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$pid = self::$configuration->bucketInfo['gd']['pid'];

		$attr = $this->getAttributeByTitle($pid, 'Id (Categories)');

		$attrUriArr = explode('/', $attr['attribute']['meta']['uri']);
		$objId = $attrUriArr[count($attrUriArr)-1];

		// repost attribute to GD

		$jobId = $this->_processJob('/gooddata-writer/object', array(
			'writerId'  => $this->writerId,
			'pid'       => $pid,
			'objectId'  => $objId,
			'object'    => $attr
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
			throw new Exception('Attributes in project could not be fetched');
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
	}
} 