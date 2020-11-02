<?php

namespace Shengfai\OpenSearch;

use Laravel\Scout\EngineManager;

/**
 * 开放搜索服务提供者
 * Class OpenSearchServiceProvider
 *
 * @package \Shengfai\OpenSearch
 * @author ShengFai <shengfai@qq.com>
 */
class OpenSearchServiceProvider
{

    /**
     * register
     */
    public function register()
    {
    }

    public function boot()
    {
        resolve(EngineManager::class)->extend('opensearch', function ($app) {
            return new OpenSearchEngine($app['config']);
        });
    }
}