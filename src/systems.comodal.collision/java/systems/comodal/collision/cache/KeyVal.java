package systems.comodal.collision.cache;

import java.util.Objects;

final class KeyVal<K, V> {

  final K key;
  final V val;

  KeyVal(final K key, final V val) {
    this.key = key;
    this.val = Objects.requireNonNull(val);
  }

  /**
   * Compares the specified object with this entry for equality.
   * Returns {@code true} if the given object is also a map entry and
   * the two entries' keys and values are equal. Note that key and
   * val are non-null, so equals() can be called safely on them.
   */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final KeyVal<?, ?> keyVal = (KeyVal<?, ?>) obj;
    return key.equals(keyVal.key) && val.equals(keyVal.val);
  }

  /**
   * Returns the hash code val for this map entry. The hash code
   * is {@code key.hashCode() ^ val.hashCode()}. Note that key and
   * val are non-null, so hashCode() can be called safely on them.
   */
  @Override
  public int hashCode() {
    return key.hashCode() ^ val.hashCode();
  }

  /**
   * Returns a String representation of this map entry.  This
   * implementation returns the string representation of this
   * entry's key followed by the colon character ("{@code :}")
   * followed by the string representation of this entry's val.
   *
   * @return a String representation of this map entry
   */
  @Override
  public String toString() {
    return key + ":" + val;
  }
}
