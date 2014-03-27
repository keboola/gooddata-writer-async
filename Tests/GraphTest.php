<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests;

use Keboola\GoodDataWriter\DependencyInjection\Configuration;
use Keboola\GoodDataWriter\Model\Graph;
use Keboola\GoodDataWriter\Exception\GraphTtlException;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

class GraphTest extends WebTestCase
{



	/**
	 * Setup called before every test
	 */
	protected function setUp()
	{
		$this->configuration = new Configuration();
	}

	public function testAddTransition()
	{
		$graph = new Graph();
		$paths = array();
		$graph->addTransition($paths, 'A', 'B');
		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'B',
					'final' => false,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'A',
									'tTarget' => 'B',
								),
						),
				),
		);
		$this->assertEquals($expected, $paths);
		$graph->addTransition($paths, 'A', 'D');
		$graph->addTransition($paths, 'C', 'D');
		$graph->addTransition($paths, 'A', 'C');

		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'B',
					'final' => false,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'A',
									'tTarget' => 'B',
								),
						),
				),
			1 =>
				array (
					'source' => 'A',
					'target' => 'D',
					'final' => false,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'A',
									'tTarget' => 'D',
								),
						),
				),
			2 =>
				array (
					'source' => 'C',
					'target' => 'D',
					'final' => false,
					'partial' => true,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'C',
									'tTarget' => 'D',
								),
						),
				),
			3 =>
				array (
					'source' => 'A',
					'target' => 'D',
					'final' => false,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'C',
									'tTarget' => 'D',
								),
							1 =>
								array (
									'tSource' => 'A',
									'tTarget' => 'C',
								),
						),
				),
		);
		$this->assertEquals($expected, $paths);
	}


	public function testStartPoints()
	{
		$graph = new Graph();
		$transitions = array(
			array (
				'tSource' => 'C',
				'tTarget' => 'D',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'D',
			),
			array (
				'tSource' => 'C',
				'tTarget' => 'D',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'C',
			),
		);
		$this->assertEquals(array('A'), $graph->getStartPoints($transitions));
	}

	public function testEndPoints()
	{
		$graph = new Graph();
		$transitions = array(
			array (
				'tSource' => 'C',
				'tTarget' => 'D',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'C',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'D',
			),
		);
		$this->assertEquals(array('D'), $graph->getEndPoints($transitions));
	}

	public function testTransitivePaths()
	{
		$graph = new Graph();
		$transitions = array(
			array (
				'tSource' => 'D',
				'tTarget' => 'E',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'D',
			),
			array (
				'tSource' => 'C',
				'tTarget' => 'D',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'C',
			),
			array (
				'tSource' => '0',
				'tTarget' => 'A',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'B',
			),
		);

		$transitiveTransitions = $graph->getTransitiveTransitions($transitions);
		$expected = array (
			0 =>
				array (
					'tSource' => 'A',
					'tTarget' => 'D',
				),
			1 =>
				array (
					'tSource' => 'C',
					'tTarget' => 'D',
				),
			2 =>
				array (
					'tSource' => 'A',
					'tTarget' => 'C',
				),
		);
		$this->assertEquals($expected, $transitiveTransitions);
	}

	public function testFindRoutes()
	{
		$graph = new Graph();
		$paths = array();
		$transitions = array(
			array (
				'tSource' => 'A',
				'tTarget' => 'B',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'C',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'D',
			),
			array (
				'tSource' => 'C',
				'tTarget' => 'D',
			),
		);
		$graph->findRoutes('B', $transitions, $paths);
		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'B',
					'final' => true,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'A',
									'tTarget' => 'B',
								),
						),
				),
		);
		$this->assertEquals($expected, $paths);
		$paths = array();
		$graph->findRoutes('D', $transitions, $paths);
		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'D',
					'final' => true,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'A',
									'tTarget' => 'D',
								),
						),
				),
			1 =>
				array (
					'source' => 'C',
					'target' => 'D',
					'final' => false,
					'partial' => true,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'C',
									'tTarget' => 'D',
								),
						),
				),
			2 =>
				array (
					'source' => 'A',
					'target' => 'D',
					'final' => true,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'tSource' => 'C',
									'tTarget' => 'D',
								),
							1 =>
								array (
									'tSource' => 'A',
									'tTarget' => 'C',
								),
						),
				),
		);
		$this->assertEquals($expected, $paths);
	}

	/**
	 * @expectedException \Keboola\GoodDataWriter\Exception\GraphTtlException
	 */
	public function testFindRoutesTtl()
	{
		$graph = new Graph();
		$paths = array();
		$transitions = array(
			array (
				'tSource' => 'A',
				'tTarget' => 'B',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'C',
			),
			array (
				'tSource' => 'A',
				'tTarget' => 'D',
			),
			array (
				'tSource' => 'C',
				'tTarget' => 'D',
			),
		);
		$graph->findRoutes('D', $transitions, $paths, 1);
	}

}
