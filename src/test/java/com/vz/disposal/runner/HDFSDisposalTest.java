// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.runner;

import com.vz.disposal.config.HDFSConfigEntry;
import com.vz.disposal.config.HDFSRetentionType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

public class HDFSDisposalTest {

    private void mockFileStatusGetPath(FileStatus fileStatus, Path path) {
        doAnswer(new Answer<Path>() {
            public Path answer(InvocationOnMock invocation) {
                return path;
            }
        }).when(fileStatus).getPath();
    }

    private void mockFileSystemGlobStatus(FileSystem fs, Path path, FileStatus[] files)  throws IOException {
        doAnswer(new Answer<FileStatus[]>() {
            public FileStatus[] answer(InvocationOnMock invocation) {
                return files;
            }
        }).when(fs).globStatus(eq(path));
    }

    @Test
    public void testDisposeWithDateStampFilterMultipleEntries() throws IOException {
        Path INCLUDE_PATH1 = new Path("hdfs://host:4443/projects/name/datasetgroup/datsetname/date=2019-04-23");
        Path EXTRA_FILE_ON_PATH1 = new Path("hdfs://host:4443/projects/name/datasetgroup/datsetname/oops.txt");
        Path INCLUDE_PATH2 = new Path("hdfs://host:4443/projects/name/datasetgroup/datsetname/date=2019-04-24");
        String CONFIG_PATH = "hdfs://host:4443/projects/name/datasetgroup/datsetname/date=%s";
        Path GLOB_PATH = new Path(CONFIG_PATH.replace("%s", "*"));
        FileSystem fs = Mockito.mock(FileSystem.class);
        FileStatus fileStatus1 = Mockito.mock(FileStatus.class);
        mockFileStatusGetPath(fileStatus1, INCLUDE_PATH1);

        FileStatus fileStatus2 = Mockito.mock(FileStatus.class);
        mockFileStatusGetPath(fileStatus2, INCLUDE_PATH2);

        FileStatus fileStatus3 = Mockito.mock(FileStatus.class);
        mockFileStatusGetPath(fileStatus3, EXTRA_FILE_ON_PATH1);

        mockFileSystemGlobStatus(fs, GLOB_PATH, new FileStatus[] {fileStatus1, fileStatus2, fileStatus3});

        HDFSDisposal runner = new HDFSDisposal(null, false, fs);

        HDFSConfigEntry entry = new HDFSConfigEntry();
        entry.setPath(CONFIG_PATH);
        entry.setRetentionType(HDFSRetentionType.PATH_DATE);
        entry.setDateFormat("yyyy-MM-dd");
        entry.setRetentionDuration(14);
        entry.setGranularity(ChronoUnit.DAYS);
        List<Path> dispose = runner.dispose(entry);

        Assert.assertTrue(dispose.contains(INCLUDE_PATH1));
        Assert.assertTrue(dispose.contains(INCLUDE_PATH2));
        Assert.assertEquals(dispose.size(), 2);

        verify(fs, atLeast(2)).delete(any(), eq(false));
        verify(fs, atMost(2)).delete(any(), eq(false));

    }

    @Test
    public void testDisposeAFile() throws IOException {
        Path NOT_MATCHING_FILE = new Path("hdfs://host:4443/projects/name/datasetgroup/datsetname/hi.txt");
        Path MATCHING_FILE = new Path("hdfs://host:4443/projects/name/datasetgroup/datsetname/date=2019-10-11/test.txt");
        String CONFIG_PATH = "hdfs://host:4443/projects/name/datasetgroup/datsetname/date=%s";
        Path GLOB_PATH = new Path(CONFIG_PATH.replace("%s", "*"));
        FileSystem fs = Mockito.mock(FileSystem.class);

        FileStatus fileStatus1 = Mockito.mock(FileStatus.class);
        mockFileStatusGetPath(fileStatus1, NOT_MATCHING_FILE);

        FileStatus fileStatus2 = Mockito.mock(FileStatus.class);
        mockFileStatusGetPath(fileStatus2, MATCHING_FILE);

        mockFileSystemGlobStatus(fs, GLOB_PATH, new FileStatus[] {fileStatus1, fileStatus2});

        HDFSDisposal runner = new HDFSDisposal(null, false, fs);

        HDFSConfigEntry entry = new HDFSConfigEntry();
        entry.setPath(CONFIG_PATH);
        entry.setRetentionType(HDFSRetentionType.PATH_DATE);
        entry.setDateFormat("yyyy-MM-dd");
        entry.setRetentionDuration(14);
        entry.setGranularity(ChronoUnit.DAYS);
        List<Path> dispose = runner.dispose(entry);

        Assert.assertTrue(dispose.contains(MATCHING_FILE));
        Assert.assertFalse(dispose.contains(NOT_MATCHING_FILE));
        Assert.assertEquals(dispose.size(), 1);

        verify(fs, atLeast(1)).delete(any(), eq(false));
        verify(fs, atMost(1)).delete(any(), eq(false));
    }

    @DataProvider(name = "dryRunTest")
    public Object[][] dryRunDataProvider() {
        return new Object[][] {
                {false, 1},
                {true, 0}
        };
    }

    @Test(dataProvider = "dryRunTest")
    public void testDisposeWithModificationTimePathFilterSingleEntry(boolean dryRun, int numberOfDeletes) throws IOException {
        String CONFIG_PATH = "hdfs://host:4443/projects/name/datasetgroup/datsetname/date=*";

        HDFSConfigEntry entry = new HDFSConfigEntry();
        entry.setPath(CONFIG_PATH);
        entry.setRetentionType(HDFSRetentionType.MODIFICATION_TIME);
        entry.setRetentionDuration(14);
        entry.setGranularity(ChronoUnit.DAYS);
        entry.setRecursive(true);

        FileSystem fs = Mockito.mock(FileSystem.class);
        Path INCLUDE_PATH1 = new Path("hdfs://host:4443/projects/name/datasetgroup/datsetname/date=2019-04-23");
        FileStatus fileStatus1 = Mockito.mock(FileStatus.class);
        mockFileStatusGetPath(fileStatus1, INCLUDE_PATH1);
        mockFileSystemGlobStatus(fs, new Path(CONFIG_PATH), new FileStatus[] {fileStatus1});

        HDFSDisposal runner = new HDFSDisposal(null, dryRun, fs);
        List<Path> dispose = runner.dispose(entry);

        Assert.assertTrue(dispose.contains(INCLUDE_PATH1));
        Assert.assertEquals(dispose.size(), 1);
        verify(fs, atLeast(numberOfDeletes)).delete(eq(INCLUDE_PATH1), eq(true));
        verify(fs, atMost(numberOfDeletes)).delete(any(), eq(true));
    }
}
