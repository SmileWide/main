Welcome to SMILE-WIDE
======================

SMILE-WIDE is a <a href="http://en.wikipedia.org/wiki/Bayes_network">Bayesian network</a> library. 
Initially, SMILE-WIDE is a version of the well known <a href="http://genie.sis.pitt.edu">SMILE library</a>, 
augmented <b>W</b>ith <b>I</b>ntegrated <b>D</b>istributed <b>E</b>xecution. This allows execution on very
large datasets. As SMILE-WIDE is developed, BigData-specific capabilities will surpass the standard Bayesian network interfaces.

Programmer-facing, SMILE-WIDE is a .jar library which you can include in your software. 
User-facing, it is also integrated into Hive as a UDF to provide posterior probabilities 
of missing values, given the observed values for each instance.

SMILE-WIDE is written in Java, using the underlying SMILE library, which is written in C++. 
It uses Hadoop for inference on large data. 


Contents
--------

<ul>
    <li><a href="#how-to-build-the-software">How to build the software</a></li>
    <li><a href="#how-to-run-an-example-smile-wide-hadoop-job">How to run an example SMILE-WIDE Hadoop job</a></li>
    <li><a href="#how-to-test-hive-integration">How to test Hive integration</li>
	<li><a href="#problems-and-solutions">Problems and solutions</li>
    <li><a href="#how-to-generate-javadoc-api-documentation">How to generate Javadoc API documentation</a></li>
</ul>


Please contact the authors with any questions or problems:

<!-- thanks to http://www.google.com/recaptcha/mailhide/ -->

* <a href="mailto:&#115;&#104;&#111;&#111;&#108;&#116;&#122;&#064;&#103;&#109;&#097;&#105;&#108;&#046;&#099;&#111;&#109;">&#115;&#104;&#111;&#111;&#108;&#116;&#122;&#064;&#103;&#109;&#097;&#105;&#108;&#046;&#099;&#111;&#109;</a></li>
* <strike><a href="nomailto:&#116;&#111;&#109;&#097;&#115;&#046;&#115;&#105;&#110;&#103;&#108;&#105;&#097;&#114;&#064;&#098;&#111;&#101;&#105;&#110;&#103;&#046;&#099;&#111;&#109;">&#116;&#111;&#109;&#097;&#115;&#046;&#115;&#105;&#110;&#103;&#108;&#105;&#097;&#114;&#064;&#098;&#111;&#101;&#105;&#110;&#103;&#046;&#099;&#111;&#109;</a></strike></li>
* rob cranfill<a href="http://www.google.com/recaptcha/mailhide/d?k=01Z8SMgbBZkzkDY2YgPKf1lw==&amp;c=4fFyZQPFFthx3W7IcyWhuwrFLpts6oYVdmOYzRyjvPk=" onclick="window.open('http://www.google.com/recaptcha/mailhide/d?k\07501Z8SMgbBZkzkDY2YgPKf1lw\75\75\46c\0754fFyZQPFFthx3W7IcyWhuwrFLpts6oYVdmOYzRyjvPk\075', '', 'toolbar=0,scrollbars=0,location=0,statusbar=0,menubar=0,resizable=0,width=500,height=300'); return false;" title="Reveal this e-mail address">&hellip;</a></li>
* <a href="mailto:&#109;&#046;&#097;&#046;&#100;&#101;&#106;&#111;&#110;&#103;&#104;&#064;&#103;&#109;&#097;&#105;&#108;&#046;&#099;&#111;&#109;">&#109;&#046;&#097;&#046;&#100;&#101;&#106;&#111;&#110;&#103;&#104;&#064;&#103;&#109;&#097;&#105;&#108;&#046;&#099;&#111;&#109;</a></li>


How to build the software
-------------------------

SMILE-WIDE is an Eclipse project configured to use Maven. All external dependencies
are pulled from the appropriate Maven repositories.
The code can be built from the IDE or directly from the command line. 
The basic build can be started with the following command:

```
mvn clean package</code>
```

This creates two jars in the target directory and copies the appropriate native library
to the <code>target/lib</code> directory.

The binary files are:
<ul>
<li><code>smile-wide-0.0.1-SNAPSHOT.jar</code></li>
	<ul>
		<li>
		Contains the SMILE-WIDE code
		</li>
	</ul>
</li>
<li><code>smile-wide-0.0.1-SNAPSHOT-job.jar</code></li>
	<ul>
		<li>
		Contains the SMILE-WIDE code and the core SMILE jar in its lib subdirectory.
		This makes running SMILE-WIDE-based Hadoop jobs easier, because Hadoop will automatically
		add SMILE jar to the classpath on the machines running in the cluster.
		</li>
	</ul>
</li>
<li><code>libjsmile.so</code>, <code>libjsmile.jnilib</code> or <code>jsmile.dll</code></li>
	<ul>
		<li>
		JNI library containing the C++ SMILE code</li>
		</li>
	</ul>
</ul>

It's possible to build for a platform different from the one running the Maven by overriding the <code>smile.native.platform</code>
variable. For example, when building for Hadoop on 64-bit Linux cluster with Maven or Eclipse running on OSX, the command
should be extended to: 

```
mvn clean package -Dsmile.native.platform=linux64 -Dmaven.test.skip=true
```


How to run an example SMILE-WIDE Hadoop job
-------------------------------------------

The example below executes a Hadoop job loop which learns the parameters of
probability distributions for the <code>kiva.xdsl</code> network. Note that the jar file
contains the SMILE jar in its lib directory. However, the native library 
must be explicitly added to the job's distributed cache with the Hadoop's
<code>-files</code> option. Additionally, since the specifics of EM require the access
to SMILE functionality locally, the <code>.so</code> file should be copied to the 
<code>$HADOOP_BIN/native</code> directory.

```
hadoop jar smile-wide-0.0.1-SNAPSHOT-job.jar smile.wide.algorithms.em.RunDistributedEM \
  -files em-tmp.xdsl,libjsmile.so \
  -D mapred.max.split.size=250000 -D mapred.reduce.tasks=12 \
  em.initial.netfile=kiva.xdsl em.work.netfile=em-tmp.xdsl \
  em.data.file=pitt/kiva500k.txt em.stat.file=pitt/em-out \
  em.separator=9 em.local.stat.file=em-local.txt
```

The file <code>kiva.xdsl</code> is located in the project's <code>input</code> directory; <code>pitt/kiva500k.txt</code> is in the compute cluster's HDFS.

The output of the job is the local file named <code>em-tmp.xdsl</code>, containing the modified <code>kiva.xdsl</code> network with
learned parameters.


How to test Hive integration
----------------------------

To test the Hive UDFs, execute the normal maven package build followed by <code>runscripts/hivePosteriors.sh.</code> This
creates the <code>target/hive-test</code> directory, containing all the files required for UDF test. The command to run the test is:

```
hive -f hivePosteriors.q
```

Hive will import small data file and perform four queries, each calling into SMILE-WIDE UDFs.


Problems and Solutions
----------------------

<h3>java.lang.UnsatisfiedLinkError: no jsmile in java.library.path</h3>

This exception is caused by missing native library. The platform-specific library is placed in <code>target/lib</code> during the Maven build,
but Hadoop and Hive must be made aware of its existence. This is done with the Hadoop's <code>-files</code> option or 
Hive's 'ADD FILE'. Some of SMILE-WIDE algorithms contain nontrivial local component running within the 
Hadoop's client JVM. In such case the shared library should be added to <code>$HADOOP_BIN/native directory</code>


How to generate Javadoc API documentation
-----------------------------------------

The SMILE-WIDE API Javadoc documentation can be generated from the command line. With 'javadoc' on the path,
issue the following command:

```
javadoc @options.javadoc.text
```

This will generate HTML documentation in the 'javadocs' directory.

