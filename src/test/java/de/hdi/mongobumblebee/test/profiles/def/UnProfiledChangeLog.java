package de.hdi.mongobumblebee.test.profiles.def;

import org.springframework.context.annotation.Profile;

import de.hdi.mongobumblebee.MongoBumblebeeTest;
import de.hdi.mongobumblebee.changeset.ChangeLog;
import de.hdi.mongobumblebee.changeset.ChangeSet;

/**
 * @author lstolowski
 * @since 2014-09-17
 */
@ChangeLog
public class UnProfiledChangeLog {

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Pdev1", order = "01")
	public void testChangeSet(){
		System.out.println("invoked Pdev1");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Pdev2", order = "02")
	public void testChangeSet2(){
		System.out.println("invoked Pdev2");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Pdev3", order = "03")
	public void testChangeSet3(){
		System.out.println("invoked Pdev3");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Pdev4", order = "04")
	@Profile("pro")
	public void testChangeSet4(){
		System.out.println("invoked Pdev4");
	}

	@ChangeSet(author = MongoBumblebeeTest.USER, id = "Pdev5", order = "05")
	@Profile("!pro")
	public void testChangeSet5() {
		System.out.println("invoked Pdev5");
	}
}
