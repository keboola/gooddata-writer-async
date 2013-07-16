<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-19
 */

namespace Keboola\GoodDataWriter\Exception;

use Syrup\ComponentBundle\Exception\SyrupComponentException;

class ClientException extends SyrupComponentException
{
	public function __construct($message)
	{
		parent::__construct(400, $message);
	}
}