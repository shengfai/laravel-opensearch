<?php

namespace Shengfai\OpenSearch;

use Laravel\Scout\EngineManager;
use Illuminate\Support\ServiceProvider;

/**
 * 开放搜索服务提供者
 * Class OpenSearchServiceProvider
 *
 * @package \Shengfai\OpenSearch
 * @author ShengFai <shengfai@qq.com>
 */
class OpenSearchServiceProvider extends ServiceProvider
{

    /**
     * register
     */
    public function register()
    {
    }

    /**
     * Bootstrap any application services.
     *
     * @return void
     */
    public function boot()
    {
        resolve(EngineManager::class)->extend('opensearch', function ($app) {
            return new OpenSearchEngine($app['config']);
        });
    }
}