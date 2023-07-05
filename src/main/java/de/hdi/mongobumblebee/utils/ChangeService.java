package de.hdi.mongobumblebee.utils;

import static java.util.Arrays.asList;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

import de.hdi.mongobumblebee.changeset.ChangeEntry;
import de.hdi.mongobumblebee.changeset.ChangeLog;
import de.hdi.mongobumblebee.changeset.ChangeSet;
import de.hdi.mongobumblebee.exception.MongoBumblebeeChangeSetException;

/**
 * Utilities to deal with reflections and annotations
 *
 * @author lstolowski
 * @since 27/07/2014
 */
public class ChangeService {

	private static final String DEFAULT_PROFILE = "default";

	private final String changeLogsBasePackage;
	
	private final List<String> activeProfiles;

	public ChangeService(String changeLogsBasePackage) {
		this(changeLogsBasePackage, null);
	}

	public ChangeService(String changeLogsBasePackage, Environment environment) {
		this.changeLogsBasePackage = changeLogsBasePackage;

		if (environment != null && environment.getActiveProfiles() != null && environment.getActiveProfiles().length> 0) {
			this.activeProfiles = asList(environment.getActiveProfiles());
		} else {
			this.activeProfiles = asList(DEFAULT_PROFILE);
		}
	}

	public List<Class<?>> fetchChangeLogs() {
		// Workaround for https://github.com/ronmamo/reflections/issues/373 ([0.10.2] Reflections does not detect any classes, 
		// if base class (or package prefix) is passed as argument, and application is running as a jar) 
		ConfigurationBuilder configuration = new ConfigurationBuilder()
				.forPackages(changeLogsBasePackage)
				.setInputsFilter(s -> s.startsWith(changeLogsBasePackage));
		Reflections reflections = new Reflections(configuration);
		Set<Class<?>> changeLogs = reflections.getTypesAnnotatedWith(ChangeLog.class);
		List<Class<?>> filteredChangeLogs = (List<Class<?>>) filterByActiveProfiles(changeLogs);

		Collections.sort(filteredChangeLogs, new ChangeLogComparator());

		return filteredChangeLogs;
	}

	public List<Method> fetchChangeSets(final Class<?> type) throws MongoBumblebeeChangeSetException {
		final List<Method> changeSets = filterChangeSetAnnotation(asList(type.getDeclaredMethods()));
		final List<Method> filteredChangeSets = (List<Method>) filterByActiveProfiles(changeSets);

		Collections.sort(filteredChangeSets, new ChangeSetComparator());

		return filteredChangeSets;
	}

	public boolean isRunAlwaysChangeSet(Method changesetMethod){
		if (changesetMethod.isAnnotationPresent(ChangeSet.class)){
			ChangeSet annotation = changesetMethod.getAnnotation(ChangeSet.class);
			return annotation.runAlways();
		} else {
			return false;
		}
	}

	public ChangeEntry createChangeEntry(Method changesetMethod){
		if (changesetMethod.isAnnotationPresent(ChangeSet.class)){
			ChangeSet annotation = changesetMethod.getAnnotation(ChangeSet.class);

			return new ChangeEntry(
					annotation.id(),
					annotation.author(),
					new Date(),
					changesetMethod.getDeclaringClass().getName(),
					changesetMethod.getName(),
					null);
		} else {
			return null;
		}
	}

	private boolean matchesActiveSpringProfile(AnnotatedElement element) {
		if (!isPresent(Profile.class.getName())) {
			return true;
		}
		if (!element.isAnnotationPresent(Profile.class)) {
			return true; // no-profiled changeset always matches
		}
		List<String> profiles = asList(element.getAnnotation(Profile.class).value());
		for (String profile : profiles) {
			if (profile != null && profile.length() > 0 && profile.charAt(0) == '!') {
				if (!activeProfiles.contains(profile.substring(1))) {
					return true;
				}
			} else if (activeProfiles.contains(profile)) {
				return true;
			}
		}
		return false;
	}

	private List<?> filterByActiveProfiles(Collection<? extends AnnotatedElement> annotated) {
		List<AnnotatedElement> filtered = new ArrayList<>();
		for (AnnotatedElement element : annotated) {
			if (matchesActiveSpringProfile(element)){
				filtered.add( element);
			}
		}
		return filtered;
	}

	private List<Method> filterChangeSetAnnotation(List<Method> allMethods) throws MongoBumblebeeChangeSetException {
		final Set<String> changeSetIds = new HashSet<>();
		final List<Method> changesetMethods = new ArrayList<>();
		for (final Method method : allMethods) {
			if (method.isAnnotationPresent(ChangeSet.class)) {
				String id = method.getAnnotation(ChangeSet.class).id();
				if (changeSetIds.contains(id)) {
					throw new MongoBumblebeeChangeSetException(String.format("Duplicated changeset id found: '%s'", id));
				}
				changeSetIds.add(id);
				changesetMethods.add(method);
			}
		}
		return changesetMethods;
	}
	
	private boolean isPresent(String className) {
		try {
			Class.forName(className);
			return true;
		}
		catch (Exception ex) {
			// Class or one of its dependencies is not present...
			return false;
		}
	}

}
