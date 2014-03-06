<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-02-08
 *
 */
namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedConfig;
use Symfony\Component\HttpKernel\Kernel;

class GoodDataSSOException extends \Exception
{

}


class SSO
{
	const GOODDATA_EMAIL = 'security@gooddata.com';
	const SSO_SCRIPT_PATH = '/usr/local/bin/gooddata-sso.sh';

	protected $ssoProvider;
	protected $ssoUser;
	protected $passphrase;

	public $tmpPath;
	public $emailTemplate;

	protected $gooddataHost = 'secure.gooddata.com';

	public function __construct(Configuration $configuration, AppConfiguration $appConfiguration)
	{
		$this->tmpPath = $appConfiguration->tmpPath;

		$this->ssoProvider = $appConfiguration->gd_ssoProvider;
		$this->ssoUser = $appConfiguration->gd_username;
		$this->passphrase = $appConfiguration->gd_keyPassphrase;

		if (!empty($configuration->backendUrl))
			$this->gooddataHost = $configuration->backendUrl;
	}

	public function url($targetUrl, $email, $validity = 86400)
	{
		$jsonFile = sprintf('%s/%s-%s.json', $this->tmpPath, date('Ymd-His'), uniqid());
		$signData = array(
			'email' => $email,
			'validity' => time() + $validity
		);
		file_put_contents($jsonFile, json_encode($signData));

		$command = sprintf('sudo -u root %s %s %s %s %s 2>&1',
			self::SSO_SCRIPT_PATH, $this->passphrase, $jsonFile, $this->ssoUser, self::GOODDATA_EMAIL);
		shell_exec($command);
		unlink($jsonFile);
		if (file_exists($jsonFile . '.enc')) {
			$sign = file_get_contents($jsonFile . '.enc');
			unlink($jsonFile . '.enc');

			$url = sprintf("https://{$this->gooddataHost}/gdc/account/customerlogin?sessionId=%s&serverURL=%s&targetURL=%s",
				urlencode($sign),
				urlencode($this->ssoProvider),
				urlencode($targetUrl)
			);

			return $url;
		}
	}
}