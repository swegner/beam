package com.google.cloud.dataflow.sdk.transforms.display;

import com.google.cloud.dataflow.sdk.transforms.display.DisplayData.Item;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collection;

/**
 * Hamcrest matcher for making assertions on {@link DisplayData} instances.
 */
public class DisplayDataMatchers {
  /**
   * Do not instantiate.
   */
  private DisplayDataMatchers() {}

  /**
   * Creates a matcher that matches if the examined {@link DisplayData} contains any items.
   */
  public static Matcher<DisplayData> hasDisplayItem() {
    return hasDisplayItem(new AnyDisplayDataItem());
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData} contains any item
   * matching the specified {@code itemMatcher}.
   */
  public static Matcher<DisplayData> hasDisplayItem(Matcher<DisplayData.Item<?>> itemMatcher) {
    return new HasDisplayDataItemMatcher(itemMatcher);
  }

  private static class HasDisplayDataItemMatcher extends TypeSafeDiagnosingMatcher<DisplayData> {
    private final Matcher<Item<?>> itemMatcher;

    private HasDisplayDataItemMatcher(Matcher<DisplayData.Item<?>> itemMatcher) {
      this.itemMatcher = itemMatcher;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("display data with item: ");
      itemMatcher.describeTo(description);
    }

    @Override
    protected boolean matchesSafely(DisplayData data, Description mismatchDescription) {
      Collection<Item<?>> items = data.items();
      boolean isMatch = Matchers.hasItem(itemMatcher).matches(items);
      if (!isMatch) {
        mismatchDescription.appendText("found " + items.size() + " non-matching items");
      }

      return isMatch;
    }
  }

  /**
   * Creates a matcher that matches if the examined {@link DisplayData.Item} contains a key
   * with the specified value.
   */
  public static Matcher<DisplayData.Item<?>> hasKey(String key) {
    return new FeatureMatcher<DisplayData.Item<?>, String>(Matchers.is(key), "with key", "key") {
      @Override
      protected String featureValueOf(DisplayData.Item<?> actual) {
        return actual.getKey();
      }
    };
  }

  private static class AnyDisplayDataItem extends TypeSafeMatcher<DisplayData.Item<?>> {
    @Override
    public void describeTo(Description description) {
      description.appendText("<any>");
    }

    @Override
    protected boolean matchesSafely(Item<?> item) {
      return true;
    }
  }
}
