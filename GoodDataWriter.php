<?php
/**
 * GoodDataWriter.php
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-03-14
 */

namespace Keboola\GoodDataWriterBundle;

use Syrup\ComponentBundle\Component\Component;
use Keboola\StorageApi\Table;
use Symfony\Component\Stopwatch\Stopwatch;
use Keboola\GoodDataWriterBundle\Exception\WrongParametersException;

class GoodDataWriter extends Component
{
	protected $_name = 'gooddata';
	protected $_prefix = 'wr';

	public $tmpDir;
	public $configurationBucket;

	public function init($params)
	{
		// Init params
		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}

		$this->configurationBucket = $this->_configurationBucket($params['writerId']);
		if (!$this->configurationBucket) {
			throw new WrongParametersException(sprintf('WriterId \'%s\' does not exist.', $params['writerId']));
		}
	}



	public function cloneProject($params)
	{
		$this->init($params);

	}



	private function _configurationBucket($writerId)
	{
		$configurationBucket = false;
		foreach ($this->_storageApi->listBuckets() as $bucket) {
			$foundWriterType = false;
			$foundWriterName = false;
			if (isset($bucket['attributes']) && is_array($bucket['attributes'])) foreach($bucket['attributes'] as $attribute) {
				if ($attribute['name'] == 'writerId') {
					$foundWriterName = $attribute['value'] == $writerId;
				}
				if ($attribute['name'] == 'writer') {
					$foundWriterType = $attribute['value'] == $this->_name;
				}
			}
			if ($foundWriterName && $foundWriterType) {
				$configurationBucket = $bucket['id'];
				break;
			}
		}
		return $configurationBucket;
	}


}
