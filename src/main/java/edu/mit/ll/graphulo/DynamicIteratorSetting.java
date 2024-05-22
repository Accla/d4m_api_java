package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.skvi.DynamicIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Solves the problem of running out of iterator priority spaces.
 * Bundles several iterators together in one.
 * @see edu.mit.ll.graphulo.skvi.DynamicIterator
 */
public class DynamicIteratorSetting {
  private static final Logger log = LoggerFactory.getLogger(DynamicIteratorSetting.class);

  private Deque<IteratorSetting> iteratorSettingList = new LinkedList<>();
  private int diPriority;
  private String diName;
  private EnumSet<MyIteratorScope> diScopes;

  public enum MyIteratorScope {
    SCAN, MINC, MAJC_FULL, MAJC_PARTIAL;

    public static String scopesToD4mString(EnumSet<MyIteratorScope> scopes) {
      StringBuilder sb = new StringBuilder();
      for (MyIteratorScope scope : scopes) {
        sb.append(scope.name()).append(',');
      }
      return sb.toString();
    }

    public static EnumSet<MyIteratorScope> d4mStringToScopes(String s) {
      EnumSet<MyIteratorScope> scopes = EnumSet.noneOf(MyIteratorScope.class);
      for (String scope : GraphuloUtil.splitD4mString(s))
        scopes.add(MyIteratorScope.valueOf(scope.toUpperCase()));
      return scopes;
    }

    public static EnumSet<IteratorUtil.IteratorScope> getCoveringIteratorScope(EnumSet<MyIteratorScope> myscopes) {
      EnumSet<IteratorUtil.IteratorScope> scopes = EnumSet.noneOf(IteratorUtil.IteratorScope.class);
      for (MyIteratorScope myscope : myscopes) {
        switch(myscope) {
          case MAJC_FULL:
          case MAJC_PARTIAL:
            scopes.add(IteratorUtil.IteratorScope.majc);
            break;
          case MINC: scopes.add(IteratorUtil.IteratorScope.minc); break;
          case SCAN: scopes.add(IteratorUtil.IteratorScope.scan); break;
          default: throw new AssertionError();
        }
      }
      return scopes;
    }
  }

  public DynamicIteratorSetting(int diPriority, String diName) {
    this(diPriority, diName, EnumSet.allOf(MyIteratorScope.class));
  }

  public DynamicIteratorSetting(int diPriority, String diName, EnumSet<MyIteratorScope> diScopes) {
    Preconditions.checkArgument(diPriority > 0, "iterator priority must be >0: %s", diPriority);
    this.diPriority = diPriority;
    if (diName == null || diName.isEmpty())
      diName = DynamicIterator.class.getSimpleName();
    this.diName = diName;
    if (diScopes == null)
      diScopes = EnumSet.allOf(MyIteratorScope.class);
    this.diScopes = diScopes;
  }

  public int getDiPriority() {
    return diPriority;
  }

  public void setDiPriority(int diPriority) {
    this.diPriority = diPriority;
  }

  public String getDiName() {
    return diName;
  }

  public void setDiName(String diName) {
    this.diName = diName;
  }

  public EnumSet<MyIteratorScope> getDiScopes() {
    return diScopes;
  }

  public void setDiScopes(EnumSet<MyIteratorScope> diScopes) {
    this.diScopes = diScopes;
  }

  public DynamicIteratorSetting prepend(IteratorSetting setting) {
    if (setting.getIteratorClass().equals(DynamicIterator.class.getName())) {
      DynamicIteratorSetting dis = fromMap(setting.getOptions());
      for (Iterator<IteratorSetting> iterator = dis.iteratorSettingList.descendingIterator(); iterator.hasNext(); ) {
        IteratorSetting itset = iterator.next();
        iteratorSettingList.addFirst(itset);
      }
    } else
      iteratorSettingList.addFirst(setting);
    return this;
  }

  public DynamicIteratorSetting append(IteratorSetting setting) {
    if (setting.getIteratorClass().equals(DynamicIterator.class.getName())) {
      DynamicIteratorSetting dis = fromMap(setting.getOptions());
      for (IteratorSetting itset : dis.iteratorSettingList) {
        iteratorSettingList.addLast(itset);
      }
    } else
      iteratorSettingList.addLast(setting);
    return this;
  }

  public DynamicIteratorSetting clear() {
    iteratorSettingList.clear();
    return this;
  }

  public Map<String,String> buildSettingMap() {
    return buildSettingMap("");
  }

  /** Add the prefix to every setting option. */
  public Map<String,String> buildSettingMap(String pre) {
    if (pre == null) pre = "";
    Map<String,String> map = new HashMap<>();
    map.put(pre+"0.diPriority", Integer.toString(diPriority)); // 0.diPriority -> 7
    map.put(pre+"0.diName", diName);                           // 0.diName -> DynamicIterator
    map.put(pre+"0.diScopes", MyIteratorScope.scopesToD4mString(diScopes));
    int prio = 1;
    for (IteratorSetting setting : iteratorSettingList) {
      String prefix = pre+prio+"."+setting.getName()+".";
      map.put(prefix+"class", setting.getIteratorClass());        // 1.itername.class -> classname
      for (Map.Entry<String, String> entry : setting.getOptions().entrySet()) {
        map.put(prefix+"opt."+entry.getKey(), entry.getValue());  // 1.itername.opt.optkey -> optvalue
      }
      prio++;
    }
    return map;
  }

  public IteratorSetting toIteratorSetting() {
    IteratorSetting itset = new IteratorSetting(diPriority, diName, DynamicIterator.class, buildSettingMap());
    // record special ON_SCOPE option that GraphuloUtil.applyIteratorSoft recognizes
    GraphuloUtil.addOnScopeOption(itset, MyIteratorScope.getCoveringIteratorScope(diScopes));
    return itset;
  }

  public void addToScanner(ScannerBase scanner) {
    if (!iteratorSettingList.isEmpty())
      scanner.addScanIterator(toIteratorSetting());
  }

  public List<IteratorSetting> getIteratorSettingList() {
    return new ArrayList<>(iteratorSettingList);
  }

  /** Prefix is "".
   * @see #fromMap(String, Map)
   */
  public static DynamicIteratorSetting fromMap(Map<String,String> mapOrig) {
    return fromMap("", mapOrig);
  }

  /**
   * Load a DynamicIteratorSetting from a Map&lt;String,String&gt;.
   * Used inside the Accumulo iterator stack {@link SortedKeyValueIterator#init}.
   * @param pre A prefix that must be in front of every option
   * @param mapOrig Map of options. Nothing is added or removed.
   * @return New DynamicIteratorSetting
   */
  public static DynamicIteratorSetting fromMap(String pre, Map<String,String> mapOrig) {
    if (pre == null) pre = "";
    Map<String,String> mapCopy = new LinkedHashMap<>(mapOrig);
    Preconditions.checkArgument(mapOrig.containsKey(pre+"0.diPriority") && mapOrig.containsKey(pre+"0.diName"), "bad map %s", mapOrig);
    int diPriotity = Integer.parseInt(mapCopy.remove(pre+"0.diPriority"));
    String diName = mapCopy.remove(pre+"0.diName");
    EnumSet<MyIteratorScope> diScopes = MyIteratorScope.d4mStringToScopes(mapCopy.remove(pre+"0.diScopes"));
    DynamicIteratorSetting dis = new DynamicIteratorSetting(diPriotity, diName, diScopes);
    for (int prio = 1; true; prio++) {
      String prioPrefix = prio+".";
      String clazz = null, name = null, clazzStr = null, optPrefix = null;
      Map<String,String> opt = new HashMap<>();

      for (Iterator<Map.Entry<String, String>> iterator = mapCopy.entrySet().iterator(); iterator.hasNext(); ) {
        Map.Entry<String, String> entry = iterator.next();
        String key = entry.getKey();
        if (!key.startsWith(pre)) {
          iterator.remove();
          continue;
        }
        key = key.substring(pre.length());

        if (name == null && key.startsWith(prioPrefix)) {
          int idxSecondDot = key.indexOf('.', prioPrefix.length());
          Preconditions.checkArgument(idxSecondDot != -1, "invalid map entry %s -> %s", key, entry.getValue());
          name = key.substring(prioPrefix.length(), idxSecondDot);
          clazzStr = prioPrefix + name + ".class";
          optPrefix = prioPrefix + name + ".opt.";
        }

        if (name != null && key.equals(clazzStr)) {
          Preconditions.checkArgument(clazz == null, "Class defined twice: %s -> %s", key, entry.getValue());
          clazz = entry.getValue();
          iterator.remove();
        } else if (name != null && key.startsWith(optPrefix)) {
          opt.put(key.substring(optPrefix.length()), entry.getValue());
          iterator.remove();
        }
      }
      if (name == null)
        break;
      Preconditions.checkArgument(clazz != null, "no class for IteratorSetting with name %s and options %s", name, opt);
      dis.append(new IteratorSetting(prio, name, clazz, opt));
    }
    return dis;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : buildSettingMap().entrySet()) {
      sb.append(entry.getKey()).append(" -> ").append(entry.getValue()).append('\n');
    }
    return sb.toString();
  }

  /**
   * Used inside the Accumulo iterator stack to create the iterator list held in this object.
   * @see org.apache.accumulo.core.iterators.IteratorUtil#loadIterators
   * @return The iterators this object holds, loaded in order.
   */
  @SuppressWarnings("unchecked")
  public SortedKeyValueIterator<Key,Value> loadIteratorStack(SortedKeyValueIterator<Key,Value> source, IteratorEnvironment env) throws IOException {
//    if (log.isDebugEnabled())
//      if (source.getClass().equals(DynamicIterator.class))
//        log.debug("Be Careful not to reuse names! Recursive DynamicIterator: "+source);
    for (IteratorSetting setting : iteratorSettingList) {
      SortedKeyValueIterator<Key,Value> iter =
          (SortedKeyValueIterator<Key,Value>)GraphuloUtil.subclassNewInstance(
              setting.getIteratorClass(), SortedKeyValueIterator.class);
      Map<String,String> optOrig = setting.getOptions();            // the options are unmodifiable....
      Map<String,String> optCopy = new HashMap<>(optOrig.size());   // make a defensive copy so that the init function can modify them if it wants
      optCopy.putAll(optOrig);
      iter.init(source, optCopy, env);
      source = iter;
    }
    return source;
  }

}
