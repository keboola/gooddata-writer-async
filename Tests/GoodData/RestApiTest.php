<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

class RestApiTest extends \PHPUnit_Framework_TestCase
{
	/**
	 * @var \Keboola\GoodDataWriter\GoodData\RestApi
	 */
	private $_restApi;
	private $_params;
	private $_pid;
	/**
	 * @var |Monolog\Logger
	 */
	private $_log;

	public function setUp()
	{
		$this->_log = new \Monolog\Logger('test');
		$this->_log->pushHandler(new \Monolog\Handler\StreamHandler('test.log'));

		$yaml = new Symfony\Component\Yaml\Parser();
		$paramsYaml = $yaml->parse(file_get_contents($_SERVER['KERNEL_DIR'] . 'config/parameters.yml'));
		$this->_params = $paramsYaml['parameters'];

		$this->_restApi = new \Keboola\GoodDataWriter\GoodData\RestApi(null, $this->_log);
		$this->_restApi->login($this->_params['gd.username'], $this->_params['gd.password']);
	}

	public function testConfig()
	{
		$this->assertNotEmpty($this->_params['gd.username'], 'GoodData configuration in parameters.yml is incomplete');
		$this->assertNotEmpty($this->_params['gd.password'], 'GoodData configuration in parameters.yml is incomplete');
		$this->assertNotEmpty($this->_params['gd.domain'], 'GoodData configuration in parameters.yml is incomplete');
		$this->assertNotEmpty($this->_params['gd.access_token'], 'GoodData configuration in parameters.yml is incomplete');
	}

	public function testCreateAndDropProject()
	{
		$pid = $this->_restApi->createProject('Project for testing', $this->_params['gd.access_token']);
		$this->assertNotEmpty($pid);

		$projectInfo = $this->_restApi->getProject($pid);
		$this->assertNotEmpty($projectInfo);

		$result = $this->_restApi->dropProject($pid);
		$this->assertEquals(0, count($result));
	}

}
