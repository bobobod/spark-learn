package com.cczu.hadoop.hdfs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * @author yjz
 * @date 2021/11/30
 */
public class ApiDemo {
    private FileSystem fs = null;

    public void before() throws IOException, InterruptedException, URISyntaxException {
        // 连接集群 nn 地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        // 指定用户
        String user = "atguigu";
        // 获取一个客户端
        fs = FileSystem.get(uri, configuration, user);
    }

    public void after() throws IOException {
        // 关闭资源
        fs.close();
    }

    /**
     * 创建目录
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void mkdir() throws URISyntaxException, IOException, InterruptedException {
        before();
        fs.mkdirs(new Path("/xxx"));
        after();
    }

    /**
     * 上传
     * 参数优先级： 低到高
     * hdfs-default.xml => hdfs-site.xml => 资源目录下的配置文件 => 代码里面的配置
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    public void put() throws IOException, URISyntaxException, InterruptedException {
        before();
        // 参数1 是否删除源文件 2 是否覆盖目标文件（如果为false的话，目标目录下存在的文件再上传会报错） 3 源数据路径 4 目的地路径
        fs.copyFromLocalFile(false, false, new Path("D:\\xxx.txt"), new Path("/xxx"));
        after();
    }

    /**
     * 文件下载
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    public void get() throws IOException, URISyntaxException, InterruptedException {
        before();
        // 参数1 是否删除源文件 2. 源文件路径  3. 目标文件路径 4.是否开启crc文件校验 true不开启 false 开启
        fs.copyToLocalFile(false, new Path("/xxx.txt"), new Path("D:\\xxx"), false);
        after();
    }

    /**
     * 删除
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws URISyntaxException
     */
    public void delete() throws InterruptedException, IOException, URISyntaxException {
        before();
        // 参数1. 要删除的路径 2.是否递归删除
        // 删除文件
        fs.delete(new Path("/xxx.txt"), true);
        // 删除空目录
        fs.delete(new Path("/xiyou"), false);
        // 删除非空目录
        fs.delete(new Path("/jingo"), true);
        after();
    }

    /**
     * 更名和移动
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws URISyntaxException
     */
    public void mv() throws InterruptedException, IOException, URISyntaxException {
        before();
        // 文件名称的重命名
        fs.rename(new Path("/input.txt"), new Path("/input1.txt"));
        // 文件的移动和更名
        fs.rename(new Path("/input/ss.txt"), new Path("/cls.txt"));
        // 目录更名
        fs.rename(new Path("/input"), new Path("/output"));
        after();
    }

    /**
     * 文件详细信息
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws URISyntaxException
     */
    public void fileDetail() throws InterruptedException, IOException, URISyntaxException {
        before();
        // 获取文件信息
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus next = iterator.next();
            System.out.println(next.getPath());
            System.out.println(next.getPermission());
            // 获取块信息,每个块在哪个位置
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        after();
    }


}
