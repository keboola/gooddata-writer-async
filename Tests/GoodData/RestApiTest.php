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
	}

	public function testConfig()
	{
		$this->assertArrayHasKey('gooddata_writer', $this->_params, 'GoodData configuration in parameters.yml is incomplete');
		$this->assertArrayHasKey('gd', $this->_params['gooddata_writer'], 'GoodData configuration in parameters.yml is incomplete');
		$this->assertArrayHasKey('dev', $this->_params['gooddata_writer']['gd'], 'GoodData configuration in parameters.yml is incomplete');
		$this->assertArrayHasKey('username', $this->_params['gooddata_writer']['gd']['dev'], 'GoodData configuration in parameters.yml is incomplete');
		$this->assertArrayHasKey('password', $this->_params['gooddata_writer']['gd']['dev'], 'GoodData configuration in parameters.yml is incomplete');
		$this->assertArrayHasKey('domain', $this->_params['gooddata_writer']['gd']['dev'], 'GoodData configuration in parameters.yml is incomplete');
		$this->assertArrayHasKey('access_token', $this->_params['gooddata_writer']['gd']['dev'], 'GoodData configuration in parameters.yml is incomplete');

		$this->_restApi->login($this->_params['gooddata_writer']['gd']['dev']['username'], $this->_params['gooddata_writer']['gd']['dev']['password']);
	}

	/*public function testCreateProject()
	{
		$this->_pid = $this->_restApi->createProject('Project for testing', $this->_params['gd.access_token']);
		$this->assertNotEmpty($this->_pid);

		$projectInfo = $this->_restApi->getProject($this->_pid);print_r($projectInfo);
		$this->assertNotEmpty($projectInfo);
	}

	public function testCreateAndDropProject()
	{
		$result = $this->_restApi->dropProject($this->_pid);
		$this->assertEquals(0, count($result));
	}*/

}
