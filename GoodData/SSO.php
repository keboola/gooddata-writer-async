<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-02-08
 *
 */
namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Writer\Configuration;
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

	public function __construct(Configuration $configuration, array $mainConfig, $tmpPath)
	{
		$this->tmpPath = $tmpPath;

		$this->ssoProvider = $mainConfig['sso_provider'];
		$this->ssoUser = $mainConfig['username'];
		$this->passphrase = $mainConfig['key_passphrase'];

		if (!empty($configuration->backendUrl))
			$this->gooddataHost = $configuration->backendUrl;
	}

	public function url($targetUrl, $email)
	{
		$jsonFile = sprintf('%s/%s-%s.json', $this->tmpPath, date('Ymd-His'), uniqid());
		$signData = array(
			'email' => $email,
			'validity' => time() + 60 * 60 * 24
		);
		file_put_contents($jsonFile, json_encode($signData));

		shell_exec(sprintf('sudo -u root %s %s %s %s %s 2>&1',
			self::SSO_SCRIPT_PATH, $this->passphrase, $jsonFile, $this->ssoUser, self::GOODDATA_EMAIL));
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
